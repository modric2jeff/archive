module RedmineCkeditor::WikiFormatting
  module Helper
    def replace_editor_tag(field_id)
      javascript_tag <<-EOT

      //#King	//2013.09.18
      //测试
      if(!window._ckfinderLoaded) {
	      document.write('<script src="/plugin_assets/redmine_ckeditor/ckeditor/ckfinder.js?r=' + Math.random() + '" type="text/javascript"><' + '/script>');
	      window._ckfinderLoaded = true;
       }

      $(document).ready(function() {
        #{replace_editor_script(field_id)}
   

      });
      EOT
    end

    def replace_editor_script(field_id)
      <<-EOT
      (function() {
        var id = '#{field_id}';
        var textarea = document.getElementById(id);
        if (!textarea) return;

        var editor = CKEDITOR.replace(textarea, #{RedmineCkeditor.options(@project).to_json});
        //测试
        var ckfinderpath = "/ckfinder/";
        CKFinder.setupCKEditor( editor, ckfinderpath ) ;	


        editor.on("change", function() { textarea.value = editor.getSnapshot(); });
	$(window).on("beforeunload",
		function() {
			if (editor.checkDirty()) {
				return #{l(:text_warn_on_leaving_unsaved).inspect};
			}
		}
	);
	$(document).on("submit", "form",
		function() {
			editor.resetDirty();
		}
	);
      })();
      EOT
    end

    def overwrite_functions
      javascript_tag <<-EOT
        function showAndScrollTo(id, focus) {
          var elem = $("#" + id);
          elem.show();
          if (focus != null && CKEDITOR.instances.hasOwnProperty(focus)) { CKEDITOR.instances[focus].focus(); }
          $('html, body').animate({scrollTop: elem.offset().top}, 100);
        }

        function destroyEditor(id) {
          if (CKEDITOR.instances[id]) CKEDITOR.instances[id].destroy();
        }
      EOT
    end

    def initial_setup
      overwrite_functions
    end

    def wikitoolbar_for(field_id)
      if params[:format] == "js"
        javascript_tag(replace_editor_script(field_id))
      else
        ckeditor_javascripts +
        stylesheet_link_tag('editor', :plugin => 'redmine_ckeditor') +
        initial_setup + replace_editor_tag(field_id)
      end
    end

    def initial_page_content(page)
      "<h1>#{ERB::Util.html_escape page.pretty_title}</h1>"
    end

    def heads_for_wiki_formatter
    end
  end
end
