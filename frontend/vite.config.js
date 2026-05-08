import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      // 开发模式下把前端 /api 请求转发到本地 Python 工作台后端
      '/api': 'http://127.0.0.1:8765',
    },
  },
})
