const electron = require('electron');
const app = electron.app;
const BrowserWindow = electron.BrowserWindow;

var mainWindow = null;
var subpy = null;

// define function for opening a new window (.html)
exports.openWindow = (filename, width, height) => {
    let win = new BrowserWindow({width: width, height: height, minWidth: width, minHeight: height})
    win.loadURL('file://' + __dirname + '/' + filename+ '.html')
}

// define electron close
app.on('window-all-closed', function() {
    console.log("closing...")
    subpy.kill('SIGINT');
  //if (process.platform != 'darwin') {
    app.quit();
  //}
});

// define electron startup
app.on('ready', function() {
  // call python
  subpy = require('child_process').exec('python3 -m hypertag.webapi',  {
    cwd: '..'
  });
  console.log('Python Flask server started!');
  var rq = require('request-promise');
  var mainAddr = 'http://localhost:23236';

  var openWindow = function(){
    main_width = 920;
    main_height = 450;
    mainWindow = new BrowserWindow({width: main_width, height: 450, minWidth: main_width, minHeight: 450});
    console.log("DIRNAME", __dirname)
    mainWindow.loadURL('file://' + __dirname + '/hypertag.html');

    // python server call on port 6001
    // show Developer Tools in mainWindow
    //mainWindow.webContents.openDevTools();
    mainWindow.on('closed', function() {
      mainWindow = null;
    });
  };

  var startUp = function(){
    rq(mainAddr).then(function(htmlString){
        console.log('NodeJS server started!');
        openWindow();
      })
      .catch(function(err){
        //console.log('waiting for the python flask server start...');
        startUp();
      });
  };
console.log("startup");
startUp();
});
