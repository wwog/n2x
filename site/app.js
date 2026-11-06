(() => {
  const root = document.documentElement;
  const body = document.body;

  /* Theme */
  const toggle = document.getElementById("theme-toggle");
  const paintToggle = () => {
    if (!toggle) return;
    const light = root.dataset.theme === "light";
    toggle.setAttribute("aria-label", light ? "切换到深色模式" : "切换到浅色模式");
    toggle.innerHTML = light
      ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M21 12.8A8.5 8.5 0 1 1 11.2 3a6.7 6.7 0 0 0 9.8 9.8Z"/></svg>'
      : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M19.1 4.9l-1.4 1.4M6.3 17.7l-1.4 1.4"/></svg>';
  };
  paintToggle();
  toggle?.addEventListener("click", () => {
    const next = root.dataset.theme === "light" ? "dark" : "light";
    root.dataset.theme = next;
    try {
      localStorage.setItem("n2x-theme", next);
    } catch {}
    paintToggle();
  });

  /* Mobile nav */
  const menu = document.getElementById("menu-toggle");
  const nav = document.getElementById("nav");
  menu?.addEventListener("click", () => {
    nav?.classList.toggle("open");
    menu.setAttribute("aria-expanded", nav?.classList.contains("open") ? "true" : "false");
  });
  nav?.querySelectorAll("a").forEach((link) =>
    link.addEventListener("click", () => nav.classList.remove("open")),
  );

  /* Active page */
  const page = body.dataset.page;
  document.querySelectorAll(".nav a[data-page]").forEach((link) => {
    if (link.dataset.page === page) link.setAttribute("aria-current", "page");
  });

  /* Header border on scroll */
  const header = document.querySelector(".site-header");
  const onScroll = () => header?.classList.toggle("scrolled", window.scrollY > 8);
  onScroll();
  window.addEventListener("scroll", onScroll, { passive: true });

  /* Code copy buttons */
  document.querySelectorAll(".code").forEach((block) => {
    const pre = block.querySelector("pre");
    if (!pre) return;
    const lang = block.dataset.lang;
    if (lang) {
      const tag = document.createElement("span");
      tag.className = "lang";
      tag.textContent = lang;
      block.appendChild(tag);
      pre.classList.add("with-lang");
    }
    const button = document.createElement("button");
    button.className = "copy";
    button.type = "button";
    button.textContent = "复制";
    button.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(pre.innerText);
        button.textContent = "已复制";
      } catch {
        button.textContent = "复制失败";
      }
      setTimeout(() => (button.textContent = "复制"), 1400);
    });
    block.appendChild(button);
  });

  /* Reveal on scroll */
  const reveal = document.querySelectorAll(".reveal");
  if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            entry.target.classList.add("in");
            observer.unobserve(entry.target);
          }
        }
      },
      { rootMargin: "0px 0px -8% 0px", threshold: 0.06 },
    );
    reveal.forEach((el) => observer.observe(el));
  } else {
    reveal.forEach((el) => el.classList.add("in"));
  }
  // Safety net: content must never stay invisible if an observer is throttled.
  setTimeout(() => reveal.forEach((el) => el.classList.add("in")), 2400);

  /* Sidebar active section (scroll-based; works without IntersectionObserver) */
  const sideLinks = [...document.querySelectorAll(".sidebar a[href^='#']")];
  if (sideLinks.length) {
    const targets = sideLinks
      .map((link) => ({ link, el: document.querySelector(link.getAttribute("href")) }))
      .filter((target) => target.el);
    const setActive = (id) => {
      sideLinks.forEach((link) =>
        link.classList.toggle("active", link.getAttribute("href") === `#${id}`),
      );
    };
    const updateActive = () => {
      const marker = window.scrollY + 140;
      let current = targets[0]?.el.id;
      for (const target of targets) {
        if (target.el.getBoundingClientRect().top + window.scrollY <= marker) {
          current = target.el.id;
        }
      }
      if (current) setActive(current);
    };
    updateActive();
    window.addEventListener("scroll", updateActive, { passive: true });
    window.addEventListener("resize", updateActive);
  }

  const year = document.getElementById("year");
  if (year) year.textContent = new Date().getFullYear();
})();
