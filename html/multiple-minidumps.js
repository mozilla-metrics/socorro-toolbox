var kURL = "https://crash-stats.mozilla.com/api/ProcessedCrash/?datatype=processed&crash_id=";

var gData;

function go() {
  var id = d3.select("#crashID").property("value");
  var url = kURL + encodeURIComponent(id);
  d3.json(url, processData).on("error", function(e) {
    console.error(e);
    alert(e);
  });
}

function processData(d) {
  gData = d;
  var row = d3.select("#results tr");
  appendDump("main dump", d.json_dump, row);
  for (var dumpname of d.additional_minidumps) {
    appendDump(dumpname, d[dumpname].json_dump, row);
  }
}

function appendDump(name, dump, row) {
  var cell = row.append("td");
  cell.append("h2").text(name);
  var table = cell.append("table").classed("frames", true);
  var head = table.append("tr");
  head.append("td").text("Thread");
  head.append("td").text("Frame");
  head.append("td").text("Location");

  for (var thread_no in dump.threads) {
    var thread = dump.threads[thread_no];
    var irow = table.append("tr");
    irow.append("td").attr("rowspan", thread.frames.length + 1).text(thread_no);
    for (var frame_no in thread.frames) {
      var frame = thread.frames[frame_no];
      irow = table.append("tr");
      irow.append("td").text(frame_no);
      makeFrameCell(frame, irow.append("td"));
    }
  }
}

function makeFrameCell(frame, cell) {
  var label;
  label = frame.normalized;
  cell.text(label);
  return cell;
}
