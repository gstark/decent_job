// DecentJob dashboard. No framework: confirmations, bulk selection, backtrace tabs, live counts.
(function () {
  "use strict";

  var root = document.body.getAttribute("data-root") || "";

  // Confirmations on destructive forms and buttons.
  document.addEventListener("submit", function (event) {
    var form = event.target;
    var submitter = event.submitter;
    var message = (submitter && submitter.getAttribute("data-confirm")) || form.getAttribute("data-confirm");
    if (message && !window.confirm(message)) event.preventDefault();
  });

  // Bulk selection count and check-all.
  var bulk = document.querySelector("[data-bulk]");
  if (bulk) {
    var count = bulk.querySelector("[data-bulk-count]");
    var boxes = function () { return Array.prototype.slice.call(document.querySelectorAll('input[name="ids[]"]')); };
    var update = function () {
      var selected = boxes().filter(function (box) { return box.checked; }).length;
      count.textContent = selected + " selected";
    };
    document.addEventListener("change", function (event) {
      if (event.target.hasAttribute("data-check-all")) {
        boxes().forEach(function (box) { box.checked = event.target.checked; });
      }
      update();
    });
    update();
  }

  // Backtrace tabs.
  document.querySelectorAll("[data-backtrace]").forEach(function (block) {
    block.querySelectorAll("[role=tab]").forEach(function (tab) {
      tab.addEventListener("click", function () {
        block.querySelectorAll("[role=tab]").forEach(function (other) { other.setAttribute("aria-selected", other === tab ? "true" : "false"); });
        block.querySelectorAll("[data-panel]").forEach(function (panel) { panel.hidden = panel.getAttribute("data-panel") !== tab.getAttribute("data-tab"); });
      });
    });
  });

  // Label each cell with its column heading so narrow screens can stack table rows as cards.
  document.querySelectorAll("table.ledger").forEach(function (table) {
    var headings = table.querySelectorAll("thead th");
    table.querySelectorAll("tbody tr").forEach(function (row) {
      Array.prototype.forEach.call(row.children, function (cell, index) {
        var heading = headings[index] && headings[index].textContent.trim();
        if (heading) cell.setAttribute("data-label", heading);
        if (!cell.textContent.trim() && !cell.children.length) cell.classList.add("is-blank");
      });
    });
  });

  // Relative times tick without a reload.
  function relative(date) {
    var seconds = Math.round((Date.now() - date.getTime()) / 1000);
    var past = seconds >= 0;
    seconds = Math.abs(seconds);
    var text;
    if (seconds < 5) return "now";
    if (seconds < 60) text = seconds + "s";
    else if (seconds < 3600) text = Math.floor(seconds / 60) + "m";
    else if (seconds < 86400) text = Math.floor(seconds / 3600) + "h " + Math.floor((seconds % 3600) / 60) + "m";
    else text = Math.floor(seconds / 86400) + "d " + Math.floor((seconds % 86400) / 3600) + "h";
    return past ? text + " ago" : "in " + text;
  }
  function tick() {
    document.querySelectorAll("time[datetime]").forEach(function (element) {
      if (/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d/.test(element.textContent)) return; // absolute display stays absolute
      var date = new Date(element.getAttribute("datetime"));
      if (!isNaN(date)) element.textContent = relative(date);
    });
  }
  setInterval(tick, 15000);

  // Live polling of the state ribbon.
  var live = document.getElementById("live-poll");
  var ribbon = document.querySelector("[data-ribbon]");
  if (live) {
    var stored = window.localStorage.getItem("decent_job_live");
    if (stored === "off") live.checked = false;
    live.addEventListener("change", function () { window.localStorage.setItem("decent_job_live", live.checked ? "on" : "off"); });
  }
  var busy = false;
  function poll() {
    if (!live || !live.checked || !ribbon || busy || document.hidden) return;
    busy = true;
    fetch(root + "/metrics.json", { headers: { Accept: "application/json" }, credentials: "same-origin" })
      .then(function (response) { return response.ok ? response.json() : null; })
      .then(function (data) {
        if (!data) return;
        var counts = data.counts;
        var total = 0;
        Object.keys(counts).forEach(function (status) { total += counts[status]; });
        Object.keys(counts).forEach(function (status) {
          var segment = ribbon.querySelector(".seg-" + status);
          var label = ribbon.querySelector('[data-count-for="' + status + '"]');
          if (segment) {
            segment.style.flexGrow = counts[status];
            segment.style.display = counts[status] === 0 ? "none" : "";
            segment.title = status + " " + counts[status];
          }
          if (label) label.textContent = counts[status].toLocaleString();
        });
        var empty = ribbon.querySelector(".seg-empty");
        if (empty) empty.style.display = total === 0 ? "" : "none";
      })
      .catch(function () {})
      .then(function () { busy = false; });
  }
  setInterval(poll, 5000);
})();
