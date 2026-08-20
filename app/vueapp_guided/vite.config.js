import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Separate dev port so it can run alongside the existing vueapp
export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5174,
  },
})
