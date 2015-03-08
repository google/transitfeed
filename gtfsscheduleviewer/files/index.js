var map;
var infoWindow = new google.maps.InfoWindow();

// Set to true when debugging for log statements about HTTP requests.
var log = false;
var twelveHourTime = false;  // set to true to see AM/PM
var selectedRoute = null;
var forbid_editing = false;

function load(config) {
  forbid_editing = config.forbid_editing;

  sizeRouteList();
  var map_dom = document.getElementById("map");
  var mapOptions = {
    zoom: 13,
    center: config.bounds.getCenter(),
    mapTypeId : google.maps.MapTypeId.ROADMAP,
    scaleControl: true
  };
  map = new google.maps.Map(map_dom, mapOptions);
  map.fitBounds(config.bounds);
  initIcons();
  google.maps.event.addListener(map, "moveend", callbackMoveEnd);
  google.maps.event.addListener(map, "zoomend", callbackZoomEnd);
  fetchStopsInBounds(config.bounds);
  fetchRoutes();
}

function callbackZoomEnd() {
}

function callbackMoveEnd() {
  // Map moved, search for stops near the center
  fetchStopsInBounds(map.getBounds());
}

/**
 * Fetch a sample of stops in the bounding box.
 */
function fetchStopsInBounds(bounds) {
  url = "/json/boundboxstops?n=" + bounds.getNorthEast().lat()
                         + "&e=" + bounds.getNorthEast().lng()
                         + "&s=" + bounds.getSouthWest().lat()
                         + "&w=" + bounds.getSouthWest().lng()
                         + "&limit=50";
  downloadUrl(url, callbackDisplayStopsBackground);
}

/**
 * Displays stops returned by the server on the map. Expected to be called
 * when downloadUrl finishes.
 *
 * @param {String} data JSON encoded list of list, each
 *     containing a row of stops.txt
 * @param {Number} responseCode Response code from server
 */
function callbackDisplayStops(data, responseCode) {
  if (responseCode != 200) {
    return;
  }
  clearMap();
  var stops = eval(data);
  if (stops.length == 1) {
    var marker = addStopMarkerFromList(stops[0], true);
    fetchStopInfoWindow(marker);
  } else {
    for (var i=0; i<stops.length; ++i) {
      addStopMarkerFromList(stops[i], true);
    }
  }
}

function stopTextSearchSubmit() {
  var text = document.getElementById("stopTextSearchInput").value;
  var url = "/json/stopsearch?q=" + text;  // TODO URI escape
  downloadUrl(url, callbackDisplayStops);
}

function tripTextSearchSubmit() {
  var text = document.getElementById("tripTextSearchInput").value;
  selectTrip(text);
}

/**
 * Add stops markers to the map and remove stops no longer in the
 * background.
 */
function callbackDisplayStopsBackground(data, responseCode) {
  if (responseCode != 200) {
    return;
  }
  var stops = eval(data);
  // Make a list of all background markers
  var oldStopMarkers = {};
  for (var stopId in stopMarkersBackground) {
    oldStopMarkers[stopId] = stopMarkersBackground[stopId];
  }
  // Add new markers to the map and remove from oldStopMarkers
  for (var i=0; i<stops.length; ++i) {
    var marker = addStopMarkerFromList(stops[i], false);
    if (oldStopMarkers[marker.stopId]) {
      delete oldStopMarkers[marker.stopId];
    }
  }
  // Delete all markers that remain in oldStopMarkers
  for (var stopId in oldStopMarkers) {
    oldStopMarkers[stopId].setMap(null);
  }
  oldStopMarkers = {};
}

/**
 * Remove all overlays from the map
 */
function clearMap() {
  boundsOfPolyLine = null;
  for (var stopId in stopMarkersSelected) {
    stopMarkersSelected[stopId].setMap(null);
  }
  for (var stopId in stopMarkersBackground) {
    stopMarkersBackground[stopId].setMap(null);
  }
  for (var i = 0; i < existingPolylines.length; ++i) {
    existingPolylines[i].setMap(null);
  }
  stopMarkersSelected = {};
  stopMarkersBackground = {};
  existingPolylines = [];
}

/**
 * Return a new GIcon used for stops
 */
function makeStopIcon(imgUrl) {
  return {
    size: new google.maps.Size(12, 20),
    anchor: new google.maps.Point(6, 20),
    url: imgUrl
  };
}

/**
 * Initialize icons. Call once during load.
 */
function initIcons() {
  iconSelected = makeStopIcon("/file/mm_20_yellow.png");
  iconBackground = makeStopIcon("/file/mm_20_blue_trans.png");
  iconBackgroundStation = makeStopIcon("/file/mm_20_red_trans.png");
}

var iconSelected;
var iconBackground;
var iconBackgroundStation;
// Map from stopId to Marker object for stops selected because they are
// part of a trip, etc
var stopMarkersSelected = {};
// Map from stopId to Marker object for stops found by the background
// passive search
var stopMarkersBackground = {};

/**
 * Add a stop to the map, given a row from stops.txt.
 */
function addStopMarkerFromList(list, selected, text) {
  return addStopMarker(list[0], list[1], list[2], list[3], list[4], selected, text);
}

/**
 * Add a stop to the map, returning the new marker
 */
function addStopMarker(stopId, stopName, stopLat, stopLon, locationType, selected, text) {
  if (stopMarkersSelected[stopId]) {
    // stop was selected
    var marker = stopMarkersSelected[stopId];
    if (text) {
      oldText = marker.getText();
      if (oldText) {
        oldText = oldText + "<br>";
      }
      marker.setText(oldText + text);
    }
    return marker;
  }
  if (stopMarkersBackground[stopId]) {
    // Stop was in the background. Either delete it from the background or
    // leave it where it is.
    if (selected) {
      stopMarkersBackground[stopId].setMap(null);
      delete stopMarkersBackground[stopId];
    } else {
      return stopMarkersBackground[stopId];
    }
  }

  var icon;
  if (selected) {
    icon = iconSelected;
  } else if (locationType == 1)  {
    icon = iconBackgroundStation
  } else {
    icon = iconBackground;
  }
  var ll = new google.maps.LatLng(stopLat,stopLon);
  var markerOpts = {
    icon: icon,
    map: map,
    position: ll,
    draggable: !forbid_editing,
    anchorPoint: new google.maps.Point(0, -20)
  };
  var marker = null;
  if (selected || text) {
    if (!text) {
      text = "";  // Make sure every selected icon has a text box, even if empty
    }
    markerOpts.labelContent = text;
    markerOpts.labelClass = "tooltip";
    markerOpts.labelAnchor = new google.maps.Point(-10, 20);
    marker = new MarkerWithLabel(markerOpts);
  } else {
    marker = new google.maps.Marker(markerOpts);
  }
  marker.stopName = stopName;
  marker.stopId = stopId;
  if (selected) {
    stopMarkersSelected[stopId] = marker;
  } else {
    stopMarkersBackground[stopId] = marker;
  }
  google.maps.event.addListener(marker, "click", function() {
    fetchStopInfoWindow(marker);
  });
  google.maps.event.addListener(marker, "dragend", function() {
    document.getElementById("edit").style.visibility = "visible";
    document.getElementById("edit_status").innerHTML = "updating..."
    changeStopLocation(marker);
  });
  return marker;
}

/**
 * Sends new location of a stop to server.
 */
function changeStopLocation(marker) {
  var url = "/json/setstoplocation?id=" +
            encodeURIComponent(marker.stopId) +
            "&lat=" + encodeURIComponent(marker.getPosition().lat()) +
            "&lng=" + encodeURIComponent(marker.getPosition().lng());
  downloadUrl(url, function(data, responseCode) {
      document.getElementById("edit_status").innerHTML = unescape(data);
  });
}

/**
 * Saves the current state of the data file opened at server side to file.
 */
function saveData() {
  var url = "/json/savedata";
  downloadUrl(url, function(data, responseCode) {
      document.getElementById("edit_status").innerHTML = data;
  });
}

/**
 * Fetch the next departing trips from the stop for display in an info
 * window.
 */
function fetchStopInfoWindow(marker) {
  var url = "/json/stoptrips?stop=" + encodeURIComponent(marker.stopId) +
    "&time=" + parseTimeInput() + "&date=" + parseDateInput();
  downloadUrl(url, function(data, responseCode) {
      callbackDisplayStopInfoWindow(marker, data, responseCode);
  });
}

function callbackDisplayStopInfoWindow(marker, data, responseCode) {
  if (responseCode != 200) {
    return;
  }
  var timeTrips = eval(data);
  var html = "<b>" + marker.stopName + "</b> (" + marker.stopId + ")<br>";
  var latLng = marker.getPosition();
  html = html + "(" + latLng.lat() + ", " + latLng.lng() + ")<br>";
  html = html + "<table><tr><th>service_id<th>time<th>name</tr>";
  for (var i=0; i < timeTrips.length; ++i) {
    var time = timeTrips[i][0];
    var tripid = timeTrips[i][1][0];
    var tripname = timeTrips[i][1][1];
    var service_id = timeTrips[i][1][2];
    var timepoint = timeTrips[i][2];
    html = html + "<tr onClick='map.closeInfoWindow();selectTrip(\"" +
      tripid + "\")'>" +
      "<td>" + service_id +
      "<td align='right'>" + (timepoint ? "" : "~") +
      formatTime(time) + "<td>" + tripname + "</tr>";
  }
  html = html + "</table>";

  infoWindow.setContent(html);
  infoWindow.open(map, marker);
}

function leadingZero(digit) {
  if (digit < 10)
    return "0" + digit;
  else
    return "" + digit;
}

function formatTime(secSinceMidnight) {
  var hours = Math.floor(secSinceMidnight / 3600);
  var suffix = "";

  if (twelveHourTime) {
    suffix = (hours >= 12) ? "p" : "a";
    suffix += (hours >= 24) ? " next day" : "";
    hours = hours % 12;
    if (hours == 0)
      hours = 12;
  }
  var minutes = Math.floor(secSinceMidnight / 60) % 60;
  var seconds = secSinceMidnight % 60;
  if (seconds == 0) {
    return hours + ":" + leadingZero(minutes) + suffix;
  } else {
    return hours + ":" + leadingZero(minutes) + ":" + leadingZero(seconds) + suffix;
  }
}

function parseTimeInput() {
  var text = document.getElementById("timeInput").value;
  var m = text.match(/([012]?\d):([012345]?\d)(:([012345]?\d))?/);
  if (m) {
    var seconds = parseInt(m[1], 10) * 3600;
    seconds += parseInt(m[2], 10) * 60;
    if (m[4]) {
      second += parseInt(m[4], 10);
    }
    return seconds;
  } else {
    if (log)
      console.log("Couldn't match " + text + " to time");
    return "";
  }
}

function parseDateInput() {
  var text = document.getElementById("startDateInput").value;
  var m = text.match(/(19|20\d\d)(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])/);
  if (m) {
    return text;
  } else {
    if (log)
      console.log("Couldn't match " + text + " to date");
    return "";
  }
}

/**
 * Create a string of dots that gets longer with the log of count.
 */
function countToRepeatedDots(count) {
  // Find ln_2(count) + 1
  var logCount = Math.ceil(Math.log(count) / 0.693148) + 1;
  return new Array(logCount + 1).join(".");
}

function fetchRoutes() {
  url = "/json/routes";
  downloadUrl(url, callbackDisplayRoutes);
}

function callbackDisplayRoutes(data, responseCode) {
  if (responseCode != 200) {
    patternDiv.appendChild(div);
  }
  var routes = eval(data);
  var routesList = document.getElementById("routeList");
  while (routesList.hasChildNodes()) {
    routesList.removeChild(routesList.firstChild);
  }
  for (i = 0; i < routes.length; ++i) {
    var routeId = routes[i][0];
    var shortName = document.createElement("span");
    shortName.className = "shortName";
    shortName.appendChild(document.createTextNode(routes[i][1] + " "));
    var routeName = routes[i][2];
    var elem = document.createElement("div");
    elem.appendChild(shortName);
    elem.appendChild(document.createTextNode(routeName));
    elem.id = "route_" + routeId;
    elem.className = "routeChoice";
    elem.title = routeName;
    elem.addEventListener("click", makeClosure(selectRoute, routeId));

    var routeContainer = document.createElement("div");
    routeContainer.id = "route_container_" + routeId;
    routeContainer.className = "routeContainer";
    routeContainer.appendChild(elem);
    routesList.appendChild(routeContainer);
  }
}

function selectRoute(routeId) {
  var routesList = document.getElementById("routeList");
  routeSpans = routesList.getElementsByTagName("div");
  for (var i = 0; i < routeSpans.length; ++i) {
    if (routeSpans[i].className == "routeChoiceSelected") {
      routeSpans[i].className = "routeChoice";
    }
  }

  // remove any previously-expanded route
  var tripInfo = document.getElementById("tripInfo");
  if (tripInfo)
    tripInfo.parentNode.removeChild(tripInfo);

  selectedRoute = routeId;
  var span = document.getElementById("route_" + routeId);
  span.className = "routeChoiceSelected";
  fetchPatterns(routeId);
}

function fetchPatterns(routeId) {
  url = "/json/routepatterns?route=" + encodeURIComponent(routeId) + "&time=" + parseTimeInput() + "&date=" + parseDateInput();
  downloadUrl(url, callbackDisplayPatterns);
}

function callbackDisplayPatterns(data, responseCode) {
  if (responseCode != 200) {
    return;
  }
  var div = document.createElement("div");
  div.className = "tripSection";
  div.id = "tripInfo";
  var firstTrip = null;
  var patterns = eval(data);
  clearMap();
  for (i = 0; i < patterns.length; ++i) {
    patternDiv = document.createElement("div")
    patternDiv.className = 'patternSection';
    div.appendChild(patternDiv)
    var pat = patterns[i];  // [patName, patId, len(early trips), trips, len(later trips), has_non_zero_trip_type]
    if (pat[5] == '1') {
      patternDiv.className += " unusualPattern"
    }
    patternDiv.appendChild(document.createTextNode(pat[0]));
    patternDiv.appendChild(document.createTextNode(", " + (pat[2] + pat[3].length + pat[4]) + " trips: "));
    if (pat[2] > 0) {
      patternDiv.appendChild(document.createTextNode(countToRepeatedDots(pat[2]) + " "));
    }
    for (j = 0; j < pat[3].length; ++j) {
      var trip = pat[3][j];
      var tripId = trip[1];
      if ((i == 0) && (j == 0))
        firstTrip = tripId;
      patternDiv.appendChild(document.createTextNode(" "));
      var span = document.createElement("span");
      span.appendChild(document.createTextNode(formatTime(trip[0])));
      span.id = "trip_" + tripId;
      span.addEventListener("click", makeClosure(selectTrip, tripId));
      patternDiv.appendChild(span)
      span.className = "tripChoice";
    }
    if (pat[4] > 0) {
      patternDiv.appendChild(document.createTextNode(" " + countToRepeatedDots(pat[4])));
    }
    patternDiv.appendChild(document.createElement("br"));
  }
  route = document.getElementById("route_container_" + selectedRoute);
  route.appendChild(div);
  if (tripId != null)
    selectTrip(firstTrip);
}

// Needed to get around limitation in javascript scope rules.
// See http://calculist.blogspot.com/2005/12/gotcha-gotcha.html
function makeClosure(f, a, b, c) {
  return function() { f(a, b, c); };
}
function make1ArgClosure(f, a, b, c) {
  return function(x) { f(x, a, b, c); };
}
function make2ArgClosure(f, a, b, c) {
  return function(x, y) { f(x, y, a, b, c); };
}

function selectTrip(tripId) {
  var tripInfo = document.getElementById("tripInfo");
  if (tripInfo) {
    tripSpans = tripInfo.getElementsByTagName('span');
    for (var i = 0; i < tripSpans.length; ++i) {
      tripSpans[i].className = 'tripChoice';
    }
  }
  var span = document.getElementById("trip_" + tripId);
  // Won't find the span if a different route is selected
  if (span) {
    span.className = 'tripChoiceSelected';
  }
  clearMap();
  url = "/json/tripstoptimes?trip=" + encodeURIComponent(tripId);
  downloadUrl(url, callbackDisplayTripStopTimes);
  fetchTripPolyLine(tripId);
  fetchTripRows(tripId);
}

function callbackDisplayTripStopTimes(data, responseCode) {
  if (responseCode != 200) {
    return;
  }
  var stopsTimes = eval(data);
  if (!stopsTimes) return;
  displayTripStopTimes(stopsTimes[0], stopsTimes[1], stopsTimes[2]);
}

function fetchTripPolyLine(tripId) {
  url = "/json/tripshape?trip=" + encodeURIComponent(tripId);
  downloadUrl(url, callbackDisplayTripPolyLine);
}

function callbackDisplayTripPolyLine(data, responseCode) {
  if (responseCode != 200) {
    return;
  }
  var polyData = JSON.parse(data);
  if (!polyData) return;
  displayPolyLine(polyData);
}

var existingPolylines = [];
var boundsOfPolyLine = null;

function expandBoundingBox(latLng) {
  if (boundsOfPolyLine == null) {
    boundsOfPolyLine = new google.maps.LatLngBounds(latLng, latLng);
  } else {
    boundsOfPolyLine.extend(latLng);
  }
}

/**
 * Display a line given a list of points
 *
 * @param {Object} List of lat,lng pairs along with optional color.
 */
function displayPolyLine(polyData) {
  var points = polyData['points'];
  var linePoints = Array();
  for (i = 0; i < points.length; ++i) {
    var ll = new google.maps.LatLng(points[i][0], points[i][1]);
    expandBoundingBox(ll);
    linePoints[linePoints.length] = ll;
  }
  var color = '#003399';
  if (polyData['color']) {
    color = polyData['color'];
  }
  var polyline = new google.maps.Polyline({
    path: linePoints,
    strokeColor: color,
    strokeWeight: 4,
    map: map
  });
  map.fitBounds(boundsOfPolyLine);
  existingPolylines.push(polyline);
}

function displayTripStopTimes(stops, arrivalTimes, departureTimes) {
  for (i = 0; i < stops.length; ++i) {
    var marker;
    if (arrivalTimes && arrivalTimes[i] != null) {
      label = formatTime(arrivalTimes[i])
      if (departureTimes && departureTimes[i] != null &&
          arrivalTimes[i] != departureTimes[i]) {
        label += '-' + formatTime(departureTimes[i])
      }
      marker = addStopMarkerFromList(stops[i], true, label);
    } else {
      marker = addStopMarkerFromList(stops[i], true);
    }
    expandBoundingBox(marker.getPosition());
  }
  map.fitBounds(boundsOfPolyLine);
}

function fetchTripRows(tripId) {
  url = "/json/triprows?trip=" + encodeURIComponent(tripId);
  downloadUrl(url, make2ArgClosure(callbackDisplayTripRows, tripId));
}

function callbackDisplayTripRows(data, responseCode, tripId) {
  if (responseCode != 200) {
    return;
  }
  var rows = eval(data);
  if (!rows) return;
  var html = "";
  for (var i = 0; i < rows.length; ++i) {
    var filename = rows[i][0];
    var row = rows[i][1];
    html += "<b>" + filename + "</b>: " + formatDictionary(row) + "<br>";
  }
  html += svgTag("/ttablegraph?height=100&trip=" + encodeURIComponent(tripId), "height='115' width='100%'");
  var bottombarDiv = document.getElementById("bottombar");
  bottombarDiv.style.display = "block";
  bottombarDiv.style.height = "175px";
  bottombarDiv.innerHTML = html;
  sizeRouteList();
}

/**
 * Return HTML to embed a SVG object in this page. src is the location of
 * the SVG and attributes is inserted directly into the object or embed
 * tag.
 */
function svgTag(src, attributes) {
  if (navigator.userAgent.toLowerCase().indexOf("msie") != -1) {
    if (isSVGControlInstalled()) {
      return "<embed pluginspage='http://www.adobe.com/svg/viewer/install/' src='" + src + "' " + attributes +"></embed>";
    } else {
      return "<p>Please install the <a href='http://www.adobe.com/svg/viewer/install/'>Adobe SVG Viewer</a> to get SVG support in IE</p>";
    }
  } else {
    return "<object data='" + src + "' type='image/svg+xml' " + attributes + "><p>No SVG support in your browser. Try Firefox 1.5 or newer or install the <a href='http://www.adobe.com/svg/viewer/install/'>Adobe SVG Viewer</a></p></object>";
  }
}

  /**
   * Format an Array object containing key-value pairs into a human readable
   * string.
   */
  function formatDictionary(d) {
var output = "";
var first = 1;
for (var k in d) {
  if (first) {
    first = 0;
  } else {
   output += "&nbsp;&nbsp; ";
  }
  output += "<b>" + k + "</b>=" + d[k];
}
return output;
  }


  function windowHeight() {
// Standard browsers (Mozilla, Safari, etc.)
if (self.innerHeight)
  return self.innerHeight;
// IE 6
if (document.documentElement && document.documentElement.clientHeight)
  return document.documentElement.clientHeight;
// IE 5
if (document.body)
  return document.body.clientHeight;
// Just in case.
return 0;
  }

function sizeRouteList() {
  var bottombarHeight = 0;
  var bottombarDiv = document.getElementById('bottombar');
  if (bottombarDiv.style.display != 'none') {
    bottombarHeight = document.getElementById('bottombar').offsetHeight
        + document.getElementById('bottombar').style.marginTop;
  }
  var height = windowHeight() - document.getElementById('topbar').offsetHeight - 15 - bottombarHeight;
  document.getElementById('content').style.height = height + 'px';
  if (map) {
    // Without this displayPolyLine does not use the correct map size
    //map.checkResize();
  }
}

var calStartDate = new CalendarPopup();
calStartDate.setReturnFunction("setStartDate");

function maybeAddLeadingZero(number) {
  if(number > 10) {
    return number;
  }
  return '0' + number;
}

function setStartDate(y,m,d) {
  document.getElementById('startDateInput').value = y + maybeAddLeadingZero(m) + maybeAddLeadingZero(d);
}

function downloadUrl(url, callback) {
  if (log) {
    console.log(url);
  }

  var xhr = new XMLHttpRequest();
  xhr.open("GET", url, true);
  xhr.onload = function (e) {
    if (xhr.readyState === 4) {
      callback(xhr.responseText, xhr.status);
    }
  };
  xhr.onerror = function (e) {
    console.error("", xhr.status);
  };
  xhr.send(null);
}
