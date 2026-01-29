(() => {
  const $ = (sel) => document.querySelector(sel);
  const nextParam = new URLSearchParams(location.search).get('next');
  const nextUrl = nextParam || '/ui/';

  function setStatus(element, message, ok) {
    if (!element) {
      return;
    }
    element.textContent = message || '';
    element.classList.remove('ok', 'err');
    if (message) {
      element.classList.add(ok ? 'ok' : 'err');
    }
  }

  async function submitAuth(form, endpoint, statusEl) {
    const fd = new FormData(form);
    const payload = {
      username: (fd.get('username') || '').toString().trim(),
      password: (fd.get('password') || '').toString(),
    };
    if (!payload.username || !payload.password) {
      setStatus(statusEl, 'Please fill in both fields.', false);
      return;
    }
    setStatus(statusEl, 'Working...', true);
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        const msg = data?.detail || response.statusText || 'Request failed';
        setStatus(statusEl, msg, false);
        return;
      }
      if (window.Auth && typeof window.Auth.setAuth === 'function') {
        window.Auth.setAuth(data);
      }
      setStatus(statusEl, 'Success! Redirecting...', true);
      setTimeout(() => {
        location.href = nextUrl;
      }, 400);
    } catch (err) {
      setStatus(statusEl, 'Network error, please try again.', false);
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    const logout = new URLSearchParams(location.search).get('logout');
    if (logout && window.Auth) {
      window.Auth.clearAuth();
    }

    const registerForm = $('#register-form');
    const loginForm = $('#login-form');
    const registerStatus = $('#register-status');
    const loginStatus = $('#login-status');

    registerForm?.addEventListener('submit', (event) => {
      event.preventDefault();
      submitAuth(registerForm, '/auth/register', registerStatus);
    });

    loginForm?.addEventListener('submit', (event) => {
      event.preventDefault();
      submitAuth(loginForm, '/auth/login', loginStatus);
    });
  });
})();
