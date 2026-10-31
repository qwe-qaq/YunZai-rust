import puppeteer from "../../lib/puppeteer/puppeteer.js"

export class yzTestRender extends plugin {
  constructor() {
    super({
      name: "渲染链路测试",
      dsc: "验证 yz-render 后端",
      event: "message",
      rule: [{ reg: "^#出图$", fnc: "shot" }],
    })
  }

  async shot() {
    // 与真实插件（如 miao-plugin）走同一条链路：puppeteer.screenshot -> 渲染器
    const img = await puppeteer.screenshot("yz-test/render", {
      tplFile: "./plugins/yz-test-render/test.html",
    })
    if (!img) return this.reply("渲染失败")
    await this.reply(img)
  }
}
