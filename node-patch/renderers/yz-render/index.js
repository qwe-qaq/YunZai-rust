import fs from "node:fs"
import path from "node:path"
import Renderer from "../../lib/renderer/Renderer.js"

const _path = process.cwd()

/**
 * yz-render 渲染后端：把 Puppeteer 截图换成 Rust 渲染服务。
 *
 * 插件完全无感知：模板仍由 art-template 渲染成本地 HTML，
 * 只是最后「HTML -> 图片」这一步改成 POST /render。
 */
class YzRender extends Renderer {
  constructor(config = {}) {
    super({
      id: "yz-render",
      type: "image",
      render: "screenshot",
      support_script: false,
    })

    this.config = {
      endpoint: String(config.endpoint || "http://127.0.0.1:6702").replace(/\/+$/, ""),
      width: Number(config.width) || 800,
      height: Number(config.height) || 600,
      scale: Number(config.scale) || 2,
      imgType: ["png", "jpeg", "webp"].includes(config.imgType) ? config.imgType : "jpeg",
      quality: Number(config.quality) || 90,
      fullPage: config.fullPage !== false,
      selector: String(config.selector || "#container"),
      timeout: Number(config.timeout) || 20000,
    }
  }

  async screenshot(name, data = {}) {
    return Renderer.withTpl(name, data, () => this.capture(name, data))
  }

  async capture(name, data) {
    const savePath = this.dealTpl(name, data)
    if (!savePath) return false

    const imgType = data.imgType || this.config.imgType
    const body = {
      url: `file://${path.resolve(_path, savePath)}`,
      width: Number(data.width) || this.config.width,
      height: Number(data.height) || this.config.height,
      scale: this.config.scale,
      selector: this.config.selector,
      img_type: imgType,
      quality: Number(data.quality) || this.config.quality,
      full_page: this.config.fullPage,
      timeout_ms: this.config.timeout,
    }

    const started = Date.now()
    let res
    try {
      res = await fetch(`${this.config.endpoint}/render`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.config.timeout + 5000),
      })
    } catch (err) {
      logger.error(`[图片生成][${name}] 渲染服务不可用`, err)
      return false
    }

    if (!res.ok) {
      const detail = await res.text().catch(() => "")
      logger.error(`[图片生成][${name}] 渲染失败 HTTP ${res.status} ${detail}`)
      return false
    }

    const buff = Buffer.from(await res.arrayBuffer())
    if (!buff.length) {
      logger.error(`[图片生成][${name}] 渲染结果为空`)
      return false
    }

    if (data.path) {
      fs.mkdirSync(path.dirname(data.path), { recursive: true })
      fs.writeFileSync(data.path, buff)
    }

    logger.mark(
      `[图片生成][${name}] ${(buff.length / 1024).toFixed(2)}KB ${Date.now() - started}ms`,
    )

    return data.multiPage ? [buff] : buff
  }
}

/**
 * Yunzai 的渲染器加载约定：default 导出的是工厂函数，不是 class。
 * @param {object} config renderers/yz-render/config.yaml 的内容
 */
export default function (config = {}) {
  return new YzRender(config)
}
