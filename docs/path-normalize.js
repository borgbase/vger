/*
 * Cloudflare Pages may serve mdBook chapters with extensionless URLs
 * (for example /backup), but mdBook sidebar links use .html paths.
 * Ensure the current chapter gets an .active link so toc.js can inject
 * current-page section headers into the sidebar.
 */
(function () {
  function canonicalizePath(pathname) {
    if (!pathname || pathname.endsWith("/")) {
      return pathname + "index.html";
    }

    var lastSegment = pathname.substring(pathname.lastIndexOf("/") + 1);
    if (lastSegment.indexOf(".") === -1) {
      return pathname + ".html";
    }

    return pathname;
  }

  function markActiveChapterLink() {
    var currentPath = canonicalizePath(window.location.pathname);
    var links = document.querySelectorAll("#mdbook-sidebar a[href]");

    for (var i = 0; i < links.length; i++) {
      var link = links[i];
      var linkPath = canonicalizePath(new URL(link.href, window.location.origin).pathname);
      if (linkPath === currentPath) {
        link.classList.add("active");
        return;
      }
    }
  }

  // Register as a capture listener so this runs before toc.js' default
  // DOMContentLoaded handler that checks for #mdbook-sidebar .active.
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", markActiveChapterLink, {
      capture: true,
      once: true,
    });
  } else {
    markActiveChapterLink();
  }
})();
