# Gut Microbiome Atlas 深度升级计划 v3.0

> **基于 10 个竞品平台源码深度分析 + 当前平台审计**
> **目标：达到 NAR Database Issue 投稿水平**
> **制定日期：2026-04-03**

---

## 一、竞品分析核心发现

### 已分析平台（含源码/镜像研究）

| 平台 | 技术栈 | 核心亮点 | 我们缺少的 |
|------|--------|---------|-----------|
| **ResMicroDb** (NAR 2026) | jQuery+ECharts+D3 | 样本相似搜索、跨研究分析(NetMoss)、身体部位交互地图 | 跨研究元分析、ECharts交互地图 |
| **GMrepo** (NAR) | RESTful API+MkDocs | 编程接口(R/Python/Perl)、图形化数据选择器、Find Markers | API文档+示例代码、编程接口 |
| **Peryton** | Angular+Material+D3 | 弦图关联可视化、力导向网络、文献溯源 | 文献证据链接、更精细的关联量化 |
| **VMH** | ExtJS+ChemDoodle | 化学结构3D展示、虚拟滚动大表格、代谢通路拓扑 | 代谢物结构可视化、大表格优化 |
| **MetOrigin** | R Shiny | 代谢物溯源(191K)、网络Sankey可视化、统计检验 | 代谢物-菌群关联网络 |
| **gutMEGA** | PHP+jQuery | 7级分类树浏览、条件对比(Log2Ratio)、高级搜索 | 分类树浏览器、高级组合搜索 |
| **MASI** | Bootstrap+Highcharts | 菌群-药物交互、景观图(Landscape) | 药物/饮食-菌群关联 |
| **Disbiome** | Angular+Material | MedDRA疾病分类、NCBI/SILVA双分类体系 | 标准化疾病本体 |
| **gcType** | Vue.js SPA | 系统发育树、地理分布可视化 | 系统发育树展示 |
| **MicrobiomeDB** | — | Lollipop差异图、多组比较 | 已有(Phase 3-4) |

### 当前平台 vs 竞品差距分析

**我们已有的优势：**
- 数据规模领先（168K样本 vs ResMicroDb 106K）
- 统计方法齐全（Wilcoxon/t-test/LEfSe/PERMANOVA/BH FDR）
- 可视化丰富（11种D3图表）
- 中英双语国际化
- 全生命周期图谱（独创）
- Tab式功能整合（用户体验好）

**核心差距（按影响力排序）：**

| 差距 | 竞品参考 | 对论文发表影响 | 实现难度 |
|------|---------|-------------|---------|
| 1. 无编程接口/API文档 | GMrepo | ★★★★★ 审稿人必问 | ★★☆ |
| 2. 无跨研究元分析 | ResMicroDb NetMoss | ★★★★☆ 杀手级功能 | ★★★★ |
| 3. 无标准化疾病/物种本体 | Disbiome MedDRA | ★★★★☆ 数据质量 | ★★★ |
| 4. 统计结果不可导出 | GMrepo/ResMicroDb | ★★★★☆ 可复现性 | ★★☆ |
| 5. 无文献证据溯源 | Peryton | ★★★☆☆ 学术可信度 | ★★★ |
| 6. 测试覆盖率0% | — | ★★★☆☆ 代码质量 | ★★★ |
| 7. 后端仅本地运行 | 所有竞品 | ★★★★★ 可用性 | ★★★ |
| 8. 无批量数据导出 | GMrepo/gutMEGA | ★★★☆☆ 实用性 | ★★☆ |
| 9. 无微生物组健康指数 | 无（创新点） | ★★★★☆ 创新性 | ★★★★ |
| 10. 无分类树浏览器 | gutMEGA | ★★☆☆☆ 交互体验 | ★★★ |

---

## 二、升级路线图（5个阶段）

### Phase A: 学术可信度层（优先级最高，2周）
> **目标：让审稿人觉得"这是一个严肃的学术平台"**

#### A1: API 文档与编程接口 ★★★★★
**参考：GMrepo 的 Programmable Access**

**后端改动：**
- 启用 FastAPI 自带的 Swagger UI（`/docs`）和 ReDoc（`/redoc`）
- 为每个端点添加详细的 docstring、参数说明、响应示例
- 添加 `/api/v1/` 版本前缀
- 添加速率限制（slowapi，60次/分钟）

**前端改动：**
- 新页面 `/api-docs`：嵌入式 API 文档（不用 Swagger 原生 UI，自定义风格）
- 每个端点配套 Python/R 示例代码（复制即用）
- 类似 GMrepo 的交互式 API 测试面板

**示例端点文档：**
```
GET /api/v1/species-profile?genus=Bacteroides

Response:
{
  "genus": "Bacteroides",
  "total_samples": 168464,
  "present_samples": 142301,
  "prevalence": 0.845,
  "mean_abundance": 0.1235,
  "by_disease": [...],
  "by_country": [...],
  "by_age_group": [...]
}

Python:
import requests
r = requests.get("https://api.gutmicrobiomeatlas.org/api/v1/species-profile", 
                  params={"genus": "Bacteroides"})
data = r.json()

R:
library(httr)
r <- GET("https://api.gutmicrobiomeatlas.org/api/v1/species-profile",
         query = list(genus = "Bacteroides"))
data <- content(r)
```

**文件变更：**
- 修改: `api/main.py`（添加 docstring、版本前缀、速率限制）
- 新增: `src/pages/ApiDocsPage.tsx`（API 文档页面）
- 新增: `src/pages/ApiDocsPage.module.css`
- 修改: `src/App.tsx`（添加路由）
- 修改: `src/sections/Header.tsx`（添加导航）
- 修改: `src/i18n/locales.ts`

**Python 依赖：**
```
pip install slowapi
```

#### A2: 统计结果可导出 ★★★★☆
**参考：GMrepo 的 Download Data**

为每个分析页面添加"导出结果"按钮：

| 页面 | 导出内容 | 格式 |
|------|---------|------|
| 差异分析 | Wilcoxon/LEfSe 结果表 | CSV/TSV |
| 疾病标志物 | Marker taxa + p值 + LDA | CSV |
| 物种画像 | 各维度丰度分布 | CSV/JSON |
| 共现网络 | 相关性矩阵 | CSV |
| 生命周期 | 年龄段×属丰度 | CSV |
| 相似搜索 | Top-K 结果 | CSV |

**实现方式：**
- 前端工具函数 `exportToCSV(data, filename)`
- 每个结果区域右上角添加下载图标按钮
- 支持 CSV 和 TSV 两种格式

**文件变更：**
- 新增: `src/util/export.ts`（通用导出工具）
- 修改: 6 个分析页面/Panel 组件（添加导出按钮）

#### A3: 图表导出为图片 ★★★☆☆
**参考：ResMicroDb 的 ECharts toolbox**

- 每个 D3 图表右上角添加"保存为 PNG/SVG"按钮
- 使用 `html2canvas` 或直接 SVG 序列化
- 导出图包含标题、图例、水印（Gut Microbiome Atlas）

**文件变更：**
- 新增: `src/util/chartExport.ts`
- 修改: 所有含 SVG 图表的组件

---

### Phase B: 数据质量层（1-2周）
> **目标：提升数据的学术严谨性和可复现性**

#### B1: 疾病名称标准化（本体映射）★★★★☆
**参考：Disbiome 的 MedDRA + ResMicroDb 的 EFO/MONDO/DO**

当前问题：疾病名称不规范（如 "c_difficile_infection" vs "CDI" vs "Clostridioides difficile infection"）

**方案：**
- 构建 `disease_ontology.json` 映射表：
  ```json
  {
    "c_difficile_infection": {
      "standard_name": "Clostridioides difficile infection",
      "abbreviation": "CDI",
      "mesh_id": "D003015",
      "icd10": "A04.7",
      "category": "Infectious Disease"
    }
  }
  ```
- 后端返回数据时自动映射标准名
- 前端显示标准化名称 + ICD-10 编码
- 疾病分类体系（感染性/代谢性/肿瘤/自身免疫/消化系统等）

**文件变更：**
- 新增: `api/disease_ontology.json`（218种疾病映射）
- 修改: `api/main.py`（疾病名标准化中间件）
- 修改: 前端疾病相关组件（显示标准名）

#### B2: 数据来源与引用信息 ★★★☆☆
**参考：Peryton 的文献溯源**

- 首页添加"Data Sources"区块：列出数据来自的项目/文献
- 每个 Project ID 可点击跳转到 NCBI SRA/ENA
- 添加引用建议（How to Cite）页面

**文件变更：**
- 修改: `src/sections/Overview.tsx`（添加数据来源卡片）
- 新增: `src/pages/CitePage.tsx`（引用信息）

#### B3: 数据版本与更新日志 ★★☆☆☆
- 首页显示数据版本号和最后更新日期
- `/changelog` 页面记录每次数据更新内容

---

### Phase C: 分析能力层（2-3周）
> **目标：添加竞品有但我们缺少的核心分析功能**

#### C1: 跨研究元分析 ★★★★☆
**参考：ResMicroDb 的 Cross-study Analysis**

**功能：**
- 用户选择 2+ 个研究项目（Project）
- 自动进行跨队列差异分析
- 识别在多个研究中一致显著的 marker taxa
- 结果展示：Venn 图（共享marker）+ 一致性热图

**后端：**
```python
@app.get("/api/v1/cross-study")
def cross_study_analysis(
    projects: str,  # 逗号分隔的项目ID
    disease: str,
    method: str = "wilcoxon"
):
    # 1. 按项目分组样本
    # 2. 每个项目独立做差异分析
    # 3. 合并结果，找共享显著属
    # 4. 计算一致性得分
```

**前端：**
- 新 Tab 整合到差异分析页面
- D3 Venn 图 + 一致性热图

**文件变更：**
- 修改: `api/main.py`（新端点）
- 新增: `src/pages/compare/CrossStudyPanel.tsx`
- 修改: `src/pages/ComparePage.tsx`（添加 Tab）

#### C2: 微生物组健康指数（GMHI）★★★★☆
**独创功能！参考：npj Biofilms 56岁相变论文**

**概念：**
- 基于 168K 样本训练一个"肠道微生物健康指数"
- 输入：用户的属级丰度向量
- 输出：0-100 健康评分 + 年龄段对应的参考范围 + 偏离方向

**后端算法：**
```python
def calculate_gmhi(abundances: dict, age_group: str = None):
    # 1. 定义健康关联属（从 NC 组高频属）和疾病关联属
    # 2. 计算 H_score = log(健康属集体丰度 / 疾病属集体丰度)
    # 3. 标准化到 0-100
    # 4. 按年龄段给出参考范围
    # 5. 标注偏离方向（哪些属异常偏高/偏低）
```

**前端：**
- 整合到"相似搜索"页面作为新 Tab
- 仪表盘式展示：圆环得分 + 属级偏离雷达图

**文件变更：**
- 修改: `api/main.py`（新端点 `/api/v1/health-index`）
- 新增: `src/pages/similarity/HealthIndexPanel.tsx`
- 修改: `src/pages/SimilarityPage.tsx`（添加 Tab）

#### C3: 分类树浏览器 ★★☆☆☆
**参考：gutMEGA 的 Taxonomy Browser**

- 7级分类树（Kingdom→Phylum→Class→Order→Family→Genus→Species）
- 可折叠/展开的树形结构
- 点击任一层级显示该分类下的丰度统计
- 支持搜索过滤

**文件变更：**
- 新增: `src/pages/TaxonomyPage.tsx`（独立页面）
- 修改: `api/main.py`（新端点）

#### C4: 增强型差异分析 ★★★☆☆
**参考：ResMicroDb 的 MaAsLin2**

当前只有 Wilcoxon/t-test，添加：
- **MaAsLin2 风格**：线性混合模型，支持协变量校正（年龄、性别、国家）
- **效应量森林图增强**：多个研究的效应量对比
- **火山图交互增强**：点击基因名跳转到物种详情

**文件变更：**
- 修改: `api/main.py`（增强 diff-analysis 端点）
- 修改: 差异分析相关前端组件

---

### Phase D: 用户体验层（1-2周）
> **目标：达到现代数据库平台的交互水准**

#### D1: 全局搜索增强 ★★★☆☆
**参考：ResMicroDb 的跨维度搜索**

当前搜索只支持属名。增强为：
- 单一搜索框支持：属名 / 疾病名 / 国家名 / 项目ID
- 搜索结果分类展示（按类型分组）
- 搜索历史记录（localStorage）
- 键盘快捷键（Ctrl+K 唤起搜索）

#### D2: 数据表格增强 ★★☆☆☆
**参考：VMH 的 ExtJS Grid、ResMicroDb 的 DataTables**

- 所有数据表格支持：排序、筛选、分页
- 列宽可调整
- 表格内容可复制
- 大数据集虚拟滚动（>1000行时）

#### D3: 响应式优化 ★★☆☆☆
- 移动端完整适配（当前仅基础支持）
- 图表在小屏幕上自动简化显示
- 触摸手势支持（图表缩放/拖拽）

#### D4: 加载性能优化 ★★★☆☆
- 首屏加载时间目标 < 3秒
- 图表按需渲染（Intersection Observer）
- API 响应缓存（前端 + 后端 Redis）
- 静态数据 CDN 加速

---

### Phase E: 部署与论文准备层（2-3周）
> **目标：从"学生项目"升级为"可发表的学术平台"**

#### E1: 服务器部署 ★★★★★
**当前问题：后端仅在本地笔记本运行，cpolar 隧道不稳定**

**推荐方案：**
| 方案 | 配置 | 年费 | 适合 |
|------|------|------|------|
| 轻量级 | 2核4G 阿里云ECS | ~1,200元 | 开发测试 |
| **推荐** | **4核16G + 50G SSD** | **~3,600元** | **论文投稿** |
| 高性能 | 8核32G + 100G SSD | ~6,700元 | 高并发 |

**部署架构：**
```
用户浏览器
  → Vercel (前端静态资源，全球CDN)
  → 云服务器 (FastAPI后端)
     → Nginx 反向代理 + SSL
     → Gunicorn + Uvicorn workers
     → 数据文件 (CSV + 丰度矩阵)
     → Redis 缓存
```

#### E2: How to Cite 页面 ★★★★☆
```
If you use Gut Microbiome Atlas in your research, please cite:

Zhai J, Dai C. Gut Microbiome Atlas: a comprehensive database 
and analysis platform for the human gut microbiome. 
[Journal], [Year]. doi: [xxx]

BibTeX / RIS / EndNote 一键复制
```

#### E3: 用户反馈与统计 ★★☆☆☆
- 集成 Google Analytics 或 Umami（自托管）
- 页面底部"Was this helpful?" 反馈按钮
- 月度使用统计报告（for 论文 usage 数据）

#### E4: 论文投稿准备 ★★★★★

**目标期刊：**
| 期刊 | IF | 特点 | 建议 |
|------|-----|------|------|
| **NAR Database Issue** | 16.6 | 金标准 | 首选，需7月前提交 |
| Briefings in Bioinformatics | 13.9 | 工具类 | 备选 |
| Nucleic Acids Research (正刊) | 16.6 | 方法+数据 | 如有创新算法 |
| Bioinformatics | 6.9 | 工具 | 保底 |

**论文需要的技术亮点（与竞品差异化）：**
1. **规模优势**：168K 样本（远超 GMrepo 58K、ResMicroDb 106K）
2. **肠道专注**：最大的肠道微生物专用平台
3. **全生命周期图谱**：独创功能
4. **微生物组健康指数**：独创功能
5. **多维度分析**：统计方法种类最多
6. **编程接口**：R/Python 示例代码

---

## 三、实施优先级矩阵

```
                    高影响力
                       ↑
    A1(API文档)  ★  |  ★  C2(健康指数)
    A2(结果导出) ★  |  ★  C1(跨研究)
    E1(服务器)   ★  |  ★  B1(疾病本体)
   ─────────────────┼─────────────────→ 高难度
    A3(图表导出) ★  |  ★  C4(MaAsLin2)
    B2(数据来源) ★  |  ★  C3(分类树)
    D1(全局搜索) ★  |  ★  D4(性能)
                       ↓
                    低影响力
```

### 建议实施顺序

| 序号 | 模块 | 预计工时 | 依赖 |
|------|------|---------|------|
| 1 | **A1: API文档+编程接口** | 3天 | 无 |
| 2 | **A2: 统计结果导出** | 2天 | 无 |
| 3 | **A3: 图表PNG/SVG导出** | 1天 | 无 |
| 4 | **B1: 疾病名称标准化** | 3天 | 无 |
| 5 | **B2: 数据来源+引用页** | 1天 | 无 |
| 6 | **C2: 微生物健康指数** | 4天 | 无 |
| 7 | **C1: 跨研究元分析** | 5天 | B1 |
| 8 | **E1: 服务器部署** | 2天 | 导师审批预算 |
| 9 | **D1: 全局搜索增强** | 2天 | 无 |
| 10 | **E2: How to Cite** | 0.5天 | 论文草稿完成 |
| 11 | **C3: 分类树浏览器** | 3天 | 无 |
| 12 | **C4: 协变量校正** | 3天 | 无 |
| 13 | **D2-D4: UX优化** | 3天 | 无 |

**总计：约 30-35 个工作日**

---

## 四、与当前项目面板的衔接

### 当前状态（已完成）
- Phase 1-2: 基础建设 ✅
- Phase 3-4: 七大功能模块 ✅
- 阶段 1-10: 所有基础优化 ✅

### 本计划新增（Phase 5-9）
| 新阶段 | 对应本文 | 核心任务 |
|--------|---------|---------|
| Phase 5 | Phase A | API文档+数据导出+图表导出 |
| Phase 6 | Phase B | 疾病本体+数据来源+版本管理 |
| Phase 7 | Phase C | 跨研究分析+健康指数+分类树 |
| Phase 8 | Phase D | 搜索增强+表格优化+性能 |
| Phase 9 | Phase E | 服务器部署+论文准备 |

---

## 五、技术选型建议

| 需求 | 当前 | 建议升级 | 原因 |
|------|------|---------|------|
| 可视化 | D3.js | D3 + ECharts（地图/仪表盘） | ECharts 交互地图更成熟 |
| API文档 | 无 | FastAPI Swagger + 自定义页面 | FastAPI 内置，零成本 |
| 数据导出 | 无 | 前端 Blob + FileSaver.js | 轻量，无需后端 |
| 图表导出 | 无 | SVG 序列化 + html2canvas | 原生支持 |
| 缓存 | 无 | lru-cache(后端) + SWR(前端) | 显著减少 API 响应时间 |
| 速率限制 | 无 | slowapi | 防止滥用 |
| 部署 | cpolar隧道 | Nginx + Gunicorn + SSL | 生产级稳定性 |

---

## 六、论文亮点与竞品差异化总结

### 与竞品的数据维度对比（论文 Table 1 素材）

| 特性 | Gut Microbiome Atlas | GMrepo | ResMicroDb | gutMEGA | Peryton |
|------|---------------------|--------|-----------|---------|---------|
| 样本数 | **168,464** | 58,903 | 106,464 | ~6,000 | — |
| 属级物种数 | **4,680** | ~1,500 | ~2,000 | 6,457 | ~1,396 |
| 疾病种类 | **217+** | 92 | 146 | ~100 | 43 |
| 国家覆盖 | **69** | ~30 | ~50 | ~20 | — |
| 年龄分层 | **8组** | 无 | 部分 | 无 | 无 |
| 全生命周期图谱 | **独创** | 无 | 无 | 无 | 无 |
| 健康指数 | **独创** | 无 | 无 | 无 | 无 |
| 统计方法 | **5种** | 2种 | 5种 | 1种 | 无 |
| 编程接口 | 计划中 | ✅ | 部分 | 无 | 无 |
| 中英双语 | ✅ | 英语 | 英语 | 英语 | 英语 |
| 样本相似搜索 | ✅ | 无 | ✅ | 无 | 无 |
| 跨研究分析 | 计划中 | 部分 | ✅ | 无 | 无 |

### 论文创新点提炼
1. **规模最大**：首个整合 168K+ 肠道宏基因组样本的综合分析平台
2. **全生命周期**：首次系统展示 8 个年龄阶段的肠道菌群组成变化轨迹
3. **微生物健康指数**：基于大规模人群数据构建的肠道微生物健康评分体系
4. **多维度交互分析**：集成差异分析/标志物发现/共现网络/弦图/相似搜索的一站式平台
5. **开放编程接口**：提供 R/Python 示例代码，支持批量数据访问

---

> **制定完成。建议从 Phase A（学术可信度层）开始实施，因为这是审稿人最关注的部分，且实现难度最低。**
