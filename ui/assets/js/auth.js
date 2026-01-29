(() => {
  const TOKEN_KEY = 'immunostream_token';
  const USERNAME_KEY = 'immunostream_username';
  const USER_ID_KEY = 'immunostream_user_id';

  function getToken() {
    return localStorage.getItem(TOKEN_KEY) || '';
  }

  function getUsername() {
    return localStorage.getItem(USERNAME_KEY) || '';
  }

  function setAuth(payload) {
    if (!payload) {
      return;
    }
    if (payload.token) {
      localStorage.setItem(TOKEN_KEY, payload.token);
    }
    if (payload.username) {
      localStorage.setItem(USERNAME_KEY, payload.username);
    }
    if (payload.user_id) {
      localStorage.setItem(USER_ID_KEY, payload.user_id);
    }
  }

  function clearAuth() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USERNAME_KEY);
    localStorage.removeItem(USER_ID_KEY);
  }

  function loginUrl() {
    const next = encodeURIComponent(
      location.pathname + location.search + location.hash
    );
    return `/ui/login.html?next=${next}`;
  }

  function requireAuth() {
    if (getToken()) {
      return true;
    }
    if (!location.pathname.endsWith('/login.html')) {
      location.href = loginUrl();
    }
    return false;
  }

  function apiFetch(url, options = {}) {
    const headers = new Headers(options.headers || {});
    const token = getToken();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
    const opts = { ...options, headers };
    return fetch(url, opts).then((response) => {
      if (response.status === 401 && !location.pathname.endsWith('/login.html')) {
        location.href = loginUrl();
      }
      return response;
    });
  }

  function wireNavAuth() {
    const loginLinks = document.querySelectorAll('[data-auth="login"]');
    const logoutLinks = document.querySelectorAll('[data-auth="logout"]');
    const userBadges = document.querySelectorAll('[data-auth="user"]');
    const loggedIn = !!getToken();

    loginLinks.forEach((loginLink) => {
      loginLink.style.display = loggedIn ? 'none' : '';
    });
    logoutLinks.forEach((logoutLink) => {
      logoutLink.style.display = loggedIn ? '' : 'none';
      if (loggedIn && !logoutLink.dataset.authBound) {
        logoutLink.addEventListener('click', (event) => {
          event.preventDefault();
          clearAuth();
          location.href = '/ui/login.html';
        });
        logoutLink.dataset.authBound = '1';
      }
    });
    userBadges.forEach((userBadge) => {
      if (loggedIn) {
        const name = getUsername();
        userBadge.textContent = name ? `Signed in: ${name}` : 'Signed in';
        userBadge.style.display = '';
      } else {
        userBadge.style.display = 'none';
      }
    });
  }

  window.Auth = {
    getToken,
    getUsername,
    setAuth,
    clearAuth,
    requireAuth,
    apiFetch,
    wireNavAuth,
  };
})();
