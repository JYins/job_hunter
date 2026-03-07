# Job Hunter V1: 我的加拿大实习找工工作流

这是一个给“我自己找 intern/co-op”用的日更流水线。  
目标很简单：每天把最值得投的岗位先排出来，少刷无效岗位，多做高价值申请动作。

这套流程会做 6 件事：
1. JobSpy 抓岗位
2. 读手动粘贴的 alert 链接
3. 标准化数据结构
4. 去重
5. 打分 + 分层（A/B/C）
6. 导出今天该看的 Excel

---

## 快速开始

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/run_daily.py
```

---

## 你每天只需要做什么

1. 把邮件里的新岗位链接贴到 `data/raw/alerts/links_today.txt`（一行一个链接）
2. 跑一次：

```bash
python scripts/run_daily.py
```

3. 打开 `data/processed/today_top_jobs.xlsx`，优先看 Top 20 和 A 类岗位

---

## 核心输入配置（先改这些）

- `data/profile/user_profile.md`
- `data/profile/skills_master.yaml`
- `data/profile/target_companies.yaml`
- `data/profile/search_config.yaml`
- `config/sources.yaml`
- `config/scoring_config.yaml`

这些文件决定了“什么岗位算适合我”。

---

## 打分怎么做（重点）

最终分数：

```text
final_score = 0.40 * rule
            + 0.20 * keyword
            + 0.20 * semantic
            + 0.10 * freshness
            + 0.10 * company
```

### 1) Rule Score（硬规则，40%）

看岗位是否满足基础门槛：
- 是否 intern/co-op/new grad 导向
- 是否在加拿大目标范围（城市或 Remote Canada）
- 是否命中 senior/staff/principal 等排除词

### 2) Keyword Score（关键词匹配，20%）

把岗位文本和你的技能词典做交集：
- 来自 `search_config.yaml` 的关键词
- 来自 `skills_master.yaml` 的技能项

匹配越多，分越高。

### 3) Semantic Score（NLP 语义匹配，20%）

这里用 `sentence-transformers/all-MiniLM-L6-v2`。  
流程是：
- 把 `user_profile.md` + 技能摘要拼成“个人画像文本”
- 把每个岗位的 `title + description` 作为岗位文本
- 生成向量后做余弦相似度（cosine similarity）
- 再把相似度映射到 0~1 分数区间

这个分的意义是：  
岗位没写出完全一样的关键词，也能识别“语义上很接近”的职位。

运行策略：
- 默认 CPU
- 如果检测到 CUDA，会自动用 GPU
- 模型加载失败时，语义分回退到 0（流水线不断）

### 4) Freshness Score（新鲜度，10%）

岗位越新，分越高。  
刚发布的岗位优先级更高，防止“晚投”。

### 5) Company Score（目标公司加权，10%）

来自 `target_companies.yaml` 的分层：
- tier_a 最高加分
- tier_b 次之
- tier_c 轻度加分

---

## 输出文件

- 主数据：`data/processed/jobs_master.csv`
- 每日看板：`data/processed/today_top_jobs.xlsx`
- 运行日志：`logs/run_YYYYMMDD.log`

Excel 里会有：
- `top_20`
- `all_scored`
- `source_summary`
- `tier_summary`

---

## 测试

```bash
pytest -q
```

当前包含：
- 1 个端到端 smoke test
- 3 个单测（去重、打分分层、语义设备选择）

---

## 小备注

这是一个“帮我更稳定投递”的系统，不是“自动海投机器人”。  
它负责筛和排优先级，我负责把高分岗位认真投出去。
