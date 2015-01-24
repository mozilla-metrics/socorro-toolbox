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

  var crashing_thread = dump.crash_info.crashing_thread;
  if (crashing_thread === undefined) {
    crashing_thread = 0;
  }

  for (var thread_no in dump.threads) {
    var thread = dump.threads[thread_no];

    var crashed = crashing_thread == thread_no;

    var irow = table.append("tr").classed("expandable", true);
    irow.append("td")
      .attr("rowspan", thread.frames.length + 1)
      .text(thread_no);
    if (crashed) {
      irow.classed("expanded", true);
    }
    for (var frame_no in thread.frames) {
      var frame = thread.frames[frame_no];
      irow = table.append("tr").classed("highlightable", true);
      if (frame_no != 0 && !crashed) {
        irow.classed("hidden", true);
      }
      irow.append("td").text(frame_no);
      makeFrameCell(frame, irow.append("td"));
    }
  }
}

function makeFrameCell(frame, cell) {
  var label;
  if (frame.function) {
    label = frame.function;
  } else if (frame.module) {
    label = frame.module + "@" + frame.offset;
  } else {
    label = "@" + frame.offset;
  }
  cell.text(label);
  cell.classed("location", true);
  cell.attr("title", label);
  if (frame.missing_symbols) {
    cell.classed("nosymbols", true);
  }
  if (frame.trust == "scan") {
    cell.classed("scanned", true);
  }
  return cell;
}

function closest(el, classname) {
  while (el) {
    if (el.classList.contains(classname)) {
      return el;
    }
    el = el.parentElement;
  }
  return null;
}

d3.select(document).on("click.expand", function() {
  var el = closest(d3.event.target, "expandable");
  console.log("xpclick", el);
  if (!el) {
    return;
  }

  var expanded = !el.classList.contains("expanded");
  el.classList.toggle("expanded", expanded);


  el = el.nextElementSibling;
  el = el && el.nextElementSibling;
  while (el && !el.classList.contains("expandable")) {
    console.log("changing visibility", el);
    el.classList.toggle("hidden", !expanded);
    el = el.nextElementSibling;
  }
});

d3.select(document).on("click.highlight", function() {
  var el = closest(d3.event.target, "highlightable");
  if (!el) {
    return;
  }

  el.classList.toggle("highlighted");
});
