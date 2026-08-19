import { marked } from 'marked'
import DOMPurify from 'dompurify'

// Render user-supplied markdown (e.g. a model README) to sanitized HTML safe
// for v-html. DOMPurify strips scripts/handlers so a malicious README can't
// inject anything executable.
export function renderMarkdown(text) {
  if (!text) return ''
  const html = marked.parse(String(text), { breaks: true, gfm: true })
  return DOMPurify.sanitize(html)
}

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

// Turn a JSON value into syntax-highlighted, pretty-printed HTML for v-html.
// The input is escaped first, then our own <span> wrappers are added — so the
// only HTML in the output is the spans we control (safe to inject).
export function highlightJson(value) {
  if (value === undefined || value === null) return ''
  const json = escapeHtml(JSON.stringify(value, null, 2))
  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false)\b|\bnull\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
    (match) => {
      let cls = 'jt-num'
      if (/^"/.test(match)) {
        cls = /:$/.test(match) ? 'jt-key' : 'jt-str'
      } else if (/true|false/.test(match)) {
        cls = 'jt-bool'
      } else if (/null/.test(match)) {
        cls = 'jt-null'
      }
      return `<span class="${cls}">${match}</span>`
    },
  )
}
