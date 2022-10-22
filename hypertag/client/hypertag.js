<!-- NeoVerse: NeoFiles the main window -->

<html>
<head>
  <link rel="stylesheet" type="text/css" href="../css/neofiles.css">
  <title>NeoVerse: NeoFiles (Pre-Alpha)</title>
</head>
<body>

  <div id="top">
    <div id="top_left">
        <div id="img_con">
            <img src="../img/logo.png" width=30 class="logo-top"/>
        </div>
        <div id="menu_con" class="top-left-menu">
            <span class="top-left-menu" style="cursor: pointer;" onclick="alert('Settings')">Settings</span>
            <span class="top-left-menu" style="cursor: pointer;" onclick="alert('About: NeoVerse - Made in Hamburg, Germany\nLead Dev: Sean Pedersen\nUI Design: Kian Shahriyari')">About</span>
        </div>
    </div>
  </div>
<div id="clear"></div>
<div id="main">
<div id="main_left">
  <input id="neosearchbar" type="text" class="file-search-bar" placeholder=" Search" onfocus="this.select();" onkeypress="handle(event)"/>
<!-- TabView -->
  <div class="tab">
    <a id="defaultOpen" href="javascript:void(0)" class="tablinks" onclick="openTab(event, 'all')"><img src="../img/eye.svg" width=18/>All</a>
    <a href="javascript:void(0)" class="tablinks" onclick="openTab(event, 'favorites')"><img src="../img/heart.svg" width=18/>Favs</a>
    <a href="javascript:void(0)" class="tablinks" onclick="openTab(event, 'history')"><img src="../img/clock.svg" width=18/>History</a>
  </div>

  <div id="all" class="tabcontent">
    <p>Display all saved NeoTags below and show all files.</p>
  </div>

  <div id="favorites" class="tabcontent">
    <p>Display favorite NeoTags, determine by most used etc.</p>
  </div>

  <div id="history" class="tabcontent">
      <p>Display search history.</p>
  </div>
</div>

<div id="main_right">
  <div id="main_right_content">
  </div>
</div>
<div id="clear"></div>
</div>

<script src="../neofiles.js"></script>
<!-- mustache template for a neofile_box (used in neofiles.js) -->
<script id="neofile-template" type="text/html">
    <div id="searchResults1" ondblclick="click_neofilebox(this)">
      <div id="searchResults1_left">
        <!--<img src="../img/Excel-icon.png" width=55 class="searchResults_icon"/> -->
      </div>
      <div id="searchResults1_right">
          <span id="neofile_title" onmouseleave="if(this.textContent.length>18){this.textContent = this.textContent.substring(0,18)+'...';}" onmouseenter="this.textContent = (this.parentNode.parentNode.parentNode.file_title);" class="searchResults1_resultTitle">{{title}}</span>
          <div id="neoTagArea">
              <div id="addMoreTags" onclick="alert(this.parentNode.parentNode.parentNode.parentNode.file_id)"><div id="addMoreTagsWrapper">+</div></div>
          </div>
      </div>
  </div>
</script>
<!-- mustache template for a neotag (used in neofiles.js) -->
<script id="neotag-template" type="text/html">
    <div id="neoTag" onmouseover="this.children[0].children[1].style.display = 'block';" onmouseout="this.children[0].children[1].style.display = 'none';">
        <div id="neoTagWrapper">
            <div id="neoTagWrapperContent" style="float:left;">{{tag_name}}</div>
            <div id="deleThisTag" style="float:right;" onclick="alert('del this')">X</div>
        </div>
    </div>
</script>
<!-- mustache template for a neotag_box (used in neofiles.js) -->
<script id="neotagbox-template" type="text/html">
    <div id="NeoTagBox"><span class="NeoTagBox_string">{{title}}</span></div>
</script>

</body>
</html>
