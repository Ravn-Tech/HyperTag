// Author: Sean Pedersen
//var nodeConsole = require('console');
function html_template(templateid, data){
  return document.getElementById( templateid ).innerHTML
    .replace(
      /{{(\w*)}}/g, // or /{(\w*)}/g for "{this} instead of %this%"
      function( m, key ){
        return data.hasOwnProperty( key ) ? data[ key ] : "";
      }
    );
}
//var myConsole = new nodeConsole.Console(process.stdout, process.stderr);

// call local python flask server backend api
function call_python(function_text, callback){
  console.log("CALLING", function_text)
  const request = new Request("http://localhost:23232/"+function_text);
  const options = {
    method: "GET",
    headers: new Headers({'content-type': 'application/json'}),
    mode: 'no-cors'
  };
  fetch(request, options)
    .then((response) => response.json())
    .then((blob) => {
      console.log("TEXT", blob)
      callback(blob);
    });
}

// Mouse Click event for neofileboxes
function click_neofilebox(e){
  console.log("CLICK ID:", e.parentNode.file_id)
    call_python("open/"+e.parentNode.file_id, function(body){  });
}

// NeoSearchBar -- Event for Enter-Key pressed (start search)
function handle(e){
    if(e.keyCode === 13){
        e.preventDefault(); // Ensure it is only this code that runs
        search();
    }
}

function search(){
    let word = document.getElementById("neosearchbar").value;
    if (word == "")
    {openTab(null, "all")}
    else {
        // add neotagbox (history element)
        let data = {
            title: word
        }
        let html = html_template("neotagbox-template", data);
        let neotagbox = document.createElement("div");
        neotagbox.innerHTML = html;
        document.getElementById("history").appendChild(neotagbox);

        // remove / or \ for successful browser transmission
        // differentiate between unix (/) and windows (\) paths
        if (word.includes("/"))
        {word = word.split('/').join("$");}
        else {
          word = word.split('\\').join("$");
        }
        word = word.replace("#", "=")
        // call python backend to get query results
        call_python("find/"+word, function(body){
            let results = body.results
            console.log("RESULTS", results)
            file_ids = eval(results);
            // render results
            document.getElementById("main_right_content").innerHTML = "";
            for (var i = 0, len = file_ids.length; i < len; i++) {
                console.log("FILE ID", file_ids[i])
                addNeoFileDOM(file_ids[i][0]);
            }
        });
    }
}

// drag N drop file/s
const holder = document.getElementById('main_right')

holder.ondragover = () => {
  return false;
}
holder.ondragleave = holder.ondragend = () => {
  return false;
}
// display file box and add file to NeoDB; upload to Cloud
holder.ondrop = (e) => {
  e.preventDefault();

  for (let f of e.dataTransfer.files) {
      //myConsole.log('File(s) dragged here: ', f.path);
      // remove / or \ for successful browser transmission
      // differentiate between unix (/) and windows (\) paths
      if (f.path.includes("/"))
      {filepath = f.path.split('/').join("$");}
      else {
        filepath = f.path.split('\\').join("$");
      }
      // extract file name
      let file_title;
      if (f.path.includes("/"))
      {  file_title = f.path.split('/')[f.path.split('/').length-1]; }
      else {
        file_title = f.path.split('\\')[f.path.split('\\').length-1];
      }
      // call python backend
      call_python("addfile/"+filepath, function(file_id){
          if(file_id == "None"){ alert(file_title + " already indexed."); }
          else if(file_id == "DIR") { alert("Folders are currently not supported."); }
          else {
              addNeoFileDOM(file_id)
          }
       });
  }
  return false;
}

function addNeoFileDOM(file_id){
    call_python("get_file_name/"+file_id, function(body){
        console.log("File NAME", body.result)
        file_suffix = body.result.split("/")[1]; // TODO: Fix use length - 1
        file_title = body.result;
        // prevent too long file_titles from getting rendered -> destorying UI
        if (file_title.length > 18)
        {
            display_file_title = file_title.substring(0, 18)+"...";
        }
        else {
            display_file_title = file_title;
        }
        // data/vars to render
        let data = {
            title: display_file_title,
        }
        // access template for neofileboxes defined in neofiles.html
        // example: http://jsfiddle.net/bu5Av/2/
        let html = html_template("neofile-template", data);
        //let html = data;
        let neofile_box = document.createElement("div");
        neofile_box.innerHTML = html;
       // neofile_box.filepath = f.path;
        //neofile_box.tags = data["tags"];
        neofile_box.file_id = file_id;// save file_id as property of the DOM object
        neofile_box.file_title = file_title;
        neofile_box.file_suffix = file_suffix;
        // add neotag/s
        call_python("get_file_tags/"+file_id, function(tags_body){
        let file_tags = eval(tags_body);
        //alert(file_tags);

        for (let i = 0, len = file_tags.length; i < len; i++) {
            //alert(file_id + " " + file_tags[i]);
            tag_data = {
                tag_name: file_tags[i]
            }
            tag_html = html_template("neotag-template", tag_data);
            let neotag = document.createElement("div");
            neotag.innerHTML = tag_html;
            neofile_box.children[0].children[1].children[1].appendChild(neotag);
        }

    });
        // add neofile_box
        document.getElementById("main_right_content").appendChild(neofile_box);
    });
}

// Tabs for NeoTags overview (All/Favorites/History) @source: https://www.w3schools.com/howto/howto_js_tabs.asp
function openTab(evt, tabName) {
    if (tabName == "all"){
        // display all NeoFiles
        call_python("files", function(files){
            console.log("FILES", files);
            let files_array = eval(files.files); // convert array (string) to JS-array
            document.getElementById("main_right_content").innerHTML = "";
            for (var i = 0, len = files_array.length; i < len; i++) {
                //alert(files_array[i]);
                addNeoFileDOM(files_array[i][1]);
            }
        });
        // display all NeoTags
        call_python("tags", function(tags){
            let tags_array = eval(tags.tags);
            document.getElementById("all").innerHTML = "";
            for (var i = 0, len = tags_array.length; i < len; i++) {
                tag_data = {
                    tag_name: tags_array[i]
                }
                tag_html = html_template("neotag-template", tag_data);
                let neotag = document.createElement("div");
                neotag.innerHTML = tag_html;
                document.getElementById("all").appendChild(neotag);
            }
        });
    }
    // Declare all variables
    var i, tabcontent, tablinks;

    // Get all elements with class="tabcontent" and hide them
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    // Get all elements with class="tablinks" and remove the class "active"
    tablinks = document.getElementsByClassName("tablinks");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = tablinks[i].className.replace(" active", "");
    }

    // Show the current tab, and add an "active" class to the link that opened the tab
    document.getElementById(tabName).style.display = "block";
    evt.currentTarget.className += " active";
}

// prevent drag/drop of files being loaded by default
document.addEventListener('dragover', event => event.preventDefault())
document.addEventListener('drop', event => event.preventDefault())

// run start up routine
document.addEventListener("DOMContentLoaded", function() {
    call_python("start", function(body){  });
    document.getElementById("defaultOpen").click();// open default tab
});
