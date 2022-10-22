const remote = require('electron').remote
const index = remote.require('./main.js')
var nodeConsole = require('console');
var myConsole = new nodeConsole.Console(process.stdout, process.stderr);

// create dynamic button
var button = document.createElement('button')
button.textContent = 'Open New Window'
// add mouse click event
button.addEventListener('click', () => {
    // open neofiles.html
    index.openWindow('neofiles', 800, 500)
    // close current window
    var window = remote.getCurrentWindow()
    window.close()
}, false)
// add button to body
document.body.appendChild(button)


// prevent drag/drop of files being loaded by default
document.addEventListener('dragover', event => event.preventDefault())
document.addEventListener('drop', event => event.preventDefault())
