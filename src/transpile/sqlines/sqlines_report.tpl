<html>
<head>
<style type="text/css">
body { font-family: verdana; }
h1 { font-family: trebuchet ms, verdana; font-size: 20px; color: #000000; border-bottom:1px solid grey; }
h2 { font-family: trebuchet ms, verdana; font-size: 18px; color: #000000; border-bottom:1px solid grey; }
p { font-size: 13px; }
table { font-size: 13px; border: 1px solid; border-collapse:collapse; }
.report_tab tr:hover td { background-color: #A4BBFE; }
th { background: #FFFFFF; padding: 0px 10px 0px 10px; border: 1px solid grey; }
td { padding: 0px 10px 0px 10px; border: 1px solid grey; }
.td_warn { background:yellow; }
.td_error { background:lightpink; }
a:link { text-decoration: none; color: #0000A0;}
a:visited { text-decoration: none; color: #0000A0;}
a:active { text-decoration: none; color: #0000A0;}
a:hover { text-decoration: none; color: #800517;}
pre { white-space: pre-wrap; } 
.token_kw { color:blue }
.token_str { color:green }
.token_num { color:brown }
.token_dtype { color:maroon  }
.token_symb { color:darkblue }
.token_comment { color:grey }
.token_func { color:blue }
.token_udf { color:#8B0000 }
.token_ok { background-color:#E0FFD2 }
.token_error { background-color:#FFCCCB }
.token_warn { background-color:#FFF380 }

.tooltip { position: relative; display: inline-block; cursor: pointer; }
.tooltip .tooltiptext {
  visibility: hidden;
  background-color: #fff;
  color: #555;
  font-family: verdana;
  font-size: 11px; 
  border-style: ridge;
  border-radius: 6px;
  padding: 5px 5px;
  position: absolute;
  z-index: 1;
  left: 100%;           /* tooltip is placed on the right without any padding */
  white-space: normal;
  top: 50%;
  opacity: 0;
  transition: opacity 0.3s;
  width: max-content;
  margin-left: -10px;  /* tooltip sligthly overlaps on the right so we can easily go to link */
  user-select: none;
}
.tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }

/* Hide collapsed rows */
#treegrid tr.hidden {
  display: none;
}

/* Indents */
#treegrid tr[aria-level="2"] > td:first-child {
  padding-left: 2.5ch;
}

#treegrid tr[aria-level="3"] > td:first-child {
  padding-left: 5ch;
}

#treegrid tr[aria-level="4"] > td:first-child {
  padding-left: 5.5ch;
}

#treegrid tr[aria-level="5"] > td:first-child {
  padding-left: 10ch;
}

/* Collapse/expand icons */
#treegrid tr > td:first-child::before {
  font-family: monospace;
  content: " ";
  display: inline-block;
  width: 2ch;
  height: 11px;
  transition: transform 0.3s;
  transform-origin: 5px 5px;
}

#treegrid tr[aria-expanded] > td:first-child::before,
#treegrid td[aria-expanded]:first-child::before {
  cursor: pointer;
  content: "\25B6";
  font-size: 0.8em;
  color: gray;
}

#treegrid tr[aria-expanded="true"] > td:first-child::before,
#treegrid td[aria-expanded="true"]:first-child::before {
  transform: rotate(90deg);
}

#tabHeaders {
  display: flex;
  border-bottom: 1px solid #ccc;
}
.tab-header {
  padding: 5px;
  margin: 0;
  cursor: pointer;
  background-color: #f1f1f1;
  border: 1px solid #ccc;
  border-bottom: none;
  display: flex;
  align-items: center;
  gap: 5px;
  font-size: 0.8em;
}
.tab-header.active {
  background-color: #fff;
  font-weight: bold;
}

#tabsContainer {
  position: relative;
  width: 100%;
  height: 500px; 
}

.tab-content {
  position: absolute;
  top: 15px;
  left: 0;
  width: 100%;
  height: calc(100% - 50px);
  display: none; /* Hidden by default */
}
.tab-content.active {
  display: block; /* Show the active tab */
}
	
iframe {
  width: 100%;
  height: 100%;
  border: none;
}
</style>

<script>
function TreeGrid(treegridElem) {

  function getAllRows() {
    var nodeList = treegridElem.querySelectorAll('tbody > tr');
    return Array.prototype.slice.call(nodeList);
  }
  
  function getContainingRow(start) {
    var possibleRow = start;
    if (treegridElem.contains(possibleRow)) {
      while (possibleRow !== treegridElem) {
        if (possibleRow.localName === 'tr') {
          return possibleRow;
        }
        possibleRow = possibleRow.parentElement;
      }
    }
  }
  
  function getLevel(row) {
    return row && parseInt(row.getAttribute('aria-level'));
  }

  function changeExpanded(doExpand, row) {
    var currentLevel = getLevel(row);
    var rows = getAllRows();
    var currentRowIndex = rows.indexOf(row);
    var didChange;
    var doExpandLevel = [];
    doExpandLevel[currentLevel + 1] = doExpand;

    while (++currentRowIndex < rows.length) {
      var nextRow = rows[currentRowIndex];
      var rowLevel = getLevel(nextRow);
      if (rowLevel <= currentLevel) {
        break; // Next row is not a level down from current row
      }
      // Only expand the next level if this level is expanded
      // and previous level is expanded
      doExpandLevel[rowLevel + 1] =
        doExpandLevel[rowLevel] && isExpanded(nextRow);
      var willHideRow = !doExpandLevel[rowLevel];
      var isRowHidden = nextRow.classList.contains('hidden');

      if (willHideRow !== isRowHidden) {
        if (willHideRow) {
          nextRow.classList.add('hidden');
        } else {
          nextRow.classList.remove('hidden');
        }
        didChange = true;
      }
    }
    if (didChange) {
      setAriaExpanded(row, doExpand);
      return true;
    }
  }

  function setAriaExpanded(row, doExpand) {
    row.setAttribute('aria-expanded', doExpand);
  }

  function isExpandable(row) {
    return row.hasAttribute('aria-expanded');
  }

  function isExpanded(row) {
    return row.getAttribute('aria-expanded') === 'true';
  }
  
  // Add detail stats tab
  function addTab(row) {
    const tabHeaders = document.getElementById('tabHeaders');
    const tabsContainer = document.getElementById('tabsContainer');
	const rowValue = row.getAttribute('row-value');
	
	if (rowValue === 'All Objects') {
	  return;
	}	
  
	// Create the tab header
    const tabHeader = document.createElement('div');
    tabHeader.className = 'tab-header';
    
    // Tab name
    const tabTitle = document.createElement('span');
    tabTitle.textContent = rowValue;
	
	// Create the close button
    const closeButton = document.createElement('button');
    closeButton.className = 'close-btn';
    closeButton.textContent = 'x';
    
    // Close button event
    closeButton.addEventListener('click', function (event) {
      event.stopPropagation(); // Prevent triggering tab activation on close
      tabHeaders.removeChild(tabHeader);
      tabsContainer.removeChild(tabContent);
    
      // If the closed tab was active, activate the first available tab
      if (tabHeader.classList.contains('active')) {
        const firstTab = tabHeaders.querySelector('.tab-header');
        if (firstTab) {
          firstTab.click();
        }
      }
    });
	
	// Tab header click event
    tabHeader.addEventListener('click', function () {
      // Deactivate all tabs
      document.querySelectorAll('.tab-header, .tab-content').forEach(el => el.classList.remove('active'));
    
      // Activate the clicked tab
      tabHeader.classList.add('active');
      tabContent.classList.add('active');
    });
	
	// Create the tab content
    const tabContent = document.createElement('div');
    tabContent.className = 'tab-content';
	
	// Create the iframe
    const iframe = document.createElement('iframe');
    iframe.src = row.getAttribute('detail-report');
	
	// Append elements
    tabHeader.appendChild(tabTitle);
	tabHeader.appendChild(closeButton);
	tabHeaders.appendChild(tabHeader);
	tabContent.appendChild(iframe);
    tabsContainer.appendChild(tabContent);
	
	// Activate the new tab
    tabHeader.click();
  }

  // Toggle row expansion if the click is over the expando triangle
  // Since the triangle is a pseudo element we can't bind an event listener to it
  function onClick(event) {
    var target = event.target;
    if (target.localName !== 'td') {
      return;
    }

    var row = getContainingRow(event.target);
    if (!isExpandable(row)) {
	  addTab(row);
      return;
    }

    // Determine if mouse coordinate is just to the left of the start of text
    var range = document.createRange();
    range.selectNodeContents(target.firstChild);
    var left = range.getBoundingClientRect().left;
    var EXPANDO_WIDTH = 20;

    if (event.clientX < left && event.clientX > left - EXPANDO_WIDTH) {
      changeExpanded(!isExpanded(row), row);
    }
	else {
	  addTab(row);
	}
  }

  // Double click on row toggles expansion
  function onDoubleClick(event) {
    var row = getContainingRow(event.target);
    if (row) {
      if (isExpandable(row)) {
        changeExpanded(!isExpanded(row), row);
      }
      event.preventDefault();
    }
  }

  treegridElem.addEventListener('click', onClick);
  treegridElem.addEventListener('dblclick', onDoubleClick);
}

document.addEventListener('DOMContentLoaded', function () {
  TreeGrid(
    document.getElementById('treegrid')
  );
  
  const tabHeaderAllObj = document.getElementById('tabHeaderAllObj');
  const tabContentAllObj = document.getElementById('tabContentAllObj');
  
  // All Objects tab header click event
  tabHeaderAllObj.addEventListener('click', function () {
    // Deactivate all tabs
    document.querySelectorAll('.tab-header, .tab-content').forEach(el => el.classList.remove('active'));
  
    // Activate the clicked tab
    tabHeaderAllObj.classList.add('active');
    tabContentAllObj.classList.add('active');
  });
});
</script>

<title>SQLines Report</title>
</head>

<body>
<?summary?>
<?detail?>
</body>
</html>