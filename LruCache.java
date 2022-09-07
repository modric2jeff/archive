package demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LruCache<K, V> {
    // positive if entries have an expiration
    private long expireAfterAccess = -1;

    // true if entries can expire after access
    private boolean entriesExpireAfterAccess;

    // positive if entries have an expiration after write
    private long expireAfterWrite = -1;

    // true if entries can expire after initial insertion
    private boolean entriesExpireAfterWrite;

    // the number of entries in the cache
    private int count = 0;
    private long maxSize = -1;


    private Map<K, Entry<K, V>> cacheData = new ConcurrentHashMap<>();

    // use CacheBuilder to construct
    LruCache() {
    }

    void setExpireAfterAccess(long expireAfterAccess) {
        if (expireAfterAccess <= 0) {
            throw new IllegalArgumentException("expireAfterAccess <= 0");
        }
        this.expireAfterAccess = expireAfterAccess;
        this.entriesExpireAfterAccess = true;
    }

    void setExpireAfterWrite(long expireAfterWrite) {
        if (expireAfterWrite <= 0) {
            throw new IllegalArgumentException("expireAfterWrite <= 0");
        }
        this.expireAfterWrite = expireAfterWrite;
        this.entriesExpireAfterWrite = true;
    }


    /**
     * The relative time used to track time-based evictions.
     *
     * @return the current relative time
     */
    protected long now() {
        // System.nanoTime takes non-negligible time, so we only use it if we need it
        // use System.nanoTime because we want relative time, not absolute time
        return entriesExpireAfterAccess || entriesExpireAfterWrite ? System.nanoTime() : 0;
    }

    // the state of an entry in the LRU list
    enum State {
        NEW, EXISTING, DELETED
    }

    static class Entry<K, V> {
        final K key;
        final V value;
        long writeTime;
        volatile long accessTime;
        Entry<K, V> before;
        Entry<K, V> after;
        State state = State.NEW;

        public Entry(K key, V value, long writeTime) {
            this.key = key;
            this.value = value;
            this.writeTime = this.accessTime = writeTime;
        }
    }


    Entry<K, V> head;
    Entry<K, V> tail;

    // lock protecting mutations to the LRU list
    private ReadWriteLock lruLock = new ReentrantReadWriteLock();

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    public V get(K key) {
        return get(key, now());
    }

    private V get(K key, long now) {
        Entry<K, V> entry = cacheData.get(key);
        if (entry == null || isExpired(entry, now)) {
            return null;
        } else {
            promote(entry, now);
            return entry.value;
        }
    }

    /**
     * Associates the specified value with the specified key in this map. If the map previously contained a mapping for
     * the key, the old value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */
    public void put(K key, V value) {
        long now = now();
        put(key, value, now);
    }

    private void put(K key, V value, long now) {
        CacheSegment<K, V> segment = getCacheSegment(key);
        Tuple<Entry<K, V>, Entry<K, V>> tuple = segment.put(key, value, now);
        boolean replaced = false;
        try (ReleasableLock ignored = lruLock.acquire()) {
            if (tuple.v2() != null && tuple.v2().state == State.EXISTING) {
                if (unlink(tuple.v2())) {
                    replaced = true;
                }
            }
            promote(tuple.v1(), now);
        }
    }

    /**
     * Invalidate the association for the specified key. A removal notification will be issued for invalidated
     * entries with {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @param key the key whose mapping is to be invalidated from the cache
     */
    public void invalidate(K key) {
        Entry<K, V> entry = cacheData.remove(key);
    }

    /**
     * The number of entries in the cache.
     *
     * @return the number of entries in the cache
     */
    public int count() {
        return count;
    }


    private boolean promote(Entry<K, V> entry, long now) {
        boolean promoted = true;
        try (ReleasableLock ignored = lruLock.acquire()) {
            switch (entry.state) {
                case DELETED:
                    promoted = false;
                    break;
                case EXISTING:
                    relinkAtHead(entry);
                    break;
                case NEW:
                    linkAtHead(entry);
                    break;
            }
            if (promoted) {
                evict(now);
            }
        }
        return promoted;
    }

    private void evict(long now) {

        while (tail != null && shouldPrune(tail, now)) {
            Entry<K, V> entry = cacheData.remove(tail.key);
            delete(entry);
        }
    }

    private void delete(Entry<K, V> entry) {
        unlink(entry);
    }

    private boolean shouldPrune(Entry<K, V> entry, long now) {
        return exceedsWeight() || isExpired(entry, now);
    }

    private boolean exceedsWeight() {
        return count > maxSize;
    }

    private boolean isExpired(Entry<K, V> entry, long now) {
        return (entriesExpireAfterAccess && now - entry.accessTime > expireAfterAccess) ||
                (entriesExpireAfterWrite && now - entry.writeTime > expireAfterWrite);
    }

    private boolean unlink(Entry<K, V> entry) {

        if (entry.state == State.EXISTING) {
            final Entry<K, V> before = entry.before;
            final Entry<K, V> after = entry.after;

            if (before == null) {
                // removing the head
                assert head == entry;
                head = after;
                if (head != null) {
                    head.before = null;
                }
            } else {
                // removing inner element
                before.after = after;
                entry.before = null;
            }

            if (after == null) {
                // removing tail
                assert tail == entry;
                tail = before;
                if (tail != null) {
                    tail.after = null;
                }
            } else {
                // removing inner element
                after.before = before;
                entry.after = null;
            }

            count--;
            entry.state = State.DELETED;
            return true;
        } else {
            return false;
        }
    }

    private void linkAtHead(Entry<K, V> entry) {

        Entry<K, V> h = head;
        entry.before = null;
        entry.after = head;
        head = entry;
        if (h == null) {
            tail = entry;
        } else {
            h.before = entry;
        }

        count++;
        entry.state = State.EXISTING;
    }

    private void relinkAtHead(Entry<K, V> entry) {

        if (head != entry) {
            unlink(entry);
            linkAtHead(entry);
        }
    }

}
