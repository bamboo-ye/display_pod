<template>
  <main class="shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">Academic Stream Processing</p>
        <h1>CVPR 近年论文大数据展示舱</h1>
      </div>
      <div class="status" :class="{ live: connected }">
        <span></span>
        {{ connected ? "实时同步中" : "等待连接" }}
      </div>
    </header>

    <section class="controls">
      <label class="search">
        <span>论文检索</span>
        <input v-model.trim="draftQuery" type="search" placeholder="标题 / 摘要关键词" @keyup.enter="applyFilters" />
      </label>
      <label>
        <span>年份</span>
        <select v-model="draftYear">
          <option value="">全部年份</option>
          <option v-for="year in years" :key="year" :value="year">{{ year }}</option>
        </select>
      </label>
      <button type="button" @click="applyFilters">查询</button>
      <button type="button" class="ghost" @click="resetFilters">重置</button>
      <button type="button" class="ghost" @click="refresh">刷新</button>
    </section>

    <section class="metrics">
      <div class="metric">
        <span>论文总量</span>
        <strong>{{ summary.papers || totalPapers }}</strong>
      </div>
      <div class="metric">
        <span>覆盖年份</span>
        <strong>{{ summary.years || coveredYears }}</strong>
      </div>
      <div class="metric">
        <span>学术实体</span>
        <strong>{{ entityCount }}</strong>
      </div>
      <div class="metric">
        <span>当前结果</span>
        <strong>{{ paperList.total || 0 }}</strong>
      </div>
    </section>

    <section class="grid">
      <ChartPanel title="年度论文趋势" :option="yearlyOption" />
      <ChartPanel title="关键词词云" :option="wordCloudOption" />
      <ChartPanel title="热门作者" :option="authorOption" />
      <ChartPanel title="热门机构" :option="institutionOption" />
    </section>

    <section class="paper-stream">
      <header class="panel__header">
        <h2>论文流</h2>
        <span>{{ listMeta }}</span>
      </header>
      <ul>
        <li v-for="paper in papers" :key="paper.source_id">
          <span>{{ paper.year }}</span>
          <button type="button" @click="selectedPaper = paper">{{ paper.title }}</button>
        </li>
      </ul>
      <footer class="pager">
        <button type="button" class="ghost" :disabled="!canPrev" @click="changePage(-1)">上一页</button>
        <button type="button" class="ghost" :disabled="!canNext" @click="changePage(1)">下一页</button>
      </footer>
    </section>

    <aside v-if="selectedPaper" class="drawer">
      <div class="drawer__content">
        <button type="button" class="drawer__close" @click="selectedPaper = null">关闭</button>
        <span class="drawer__year">{{ selectedPaper.year }}</span>
        <h2>{{ selectedPaper.title }}</h2>
        <p>{{ selectedPaper.abstract || "暂无摘要，后续真实采集会补充 abstract 字段。" }}</p>
        <div class="drawer__links">
          <a v-if="selectedPaper.paper_url" :href="selectedPaper.paper_url" target="_blank" rel="noreferrer">论文页面</a>
          <a v-if="selectedPaper.pdf_url" :href="selectedPaper.pdf_url" target="_blank" rel="noreferrer">PDF</a>
        </div>
      </div>
    </aside>
  </main>
</template>

<script setup>
import { computed, onMounted, ref } from "vue";
import ChartPanel from "./components/ChartPanel.vue";
import { fetchDashboard } from "./api/client";

const papers = ref([]);
const paperList = ref({ items: [], total: 0, limit: 12, offset: 0 });
const summary = ref({});
const years = ref([]);
const yearly = ref([]);
const keywords = ref([]);
const authors = ref([]);
const institutions = ref([]);
const connected = ref(false);
const query = ref("");
const year = ref("");
const draftQuery = ref("");
const draftYear = ref("");
const selectedPaper = ref(null);

const totalPapers = computed(() => yearly.value.reduce((sum, item) => sum + Number(item.count || 0), 0));
const coveredYears = computed(() => {
  if (!yearly.value.length) return "0";
  const years = yearly.value.map((item) => item.year);
  return `${Math.min(...years)}-${Math.max(...years)}`;
});
const entityCount = computed(() => Number(summary.value.keywords || 0) + Number(summary.value.authors || 0) + Number(summary.value.institutions || 0));
const listMeta = computed(() => {
  const start = paperList.value.total === 0 ? 0 : paperList.value.offset + 1;
  const end = Math.min(paperList.value.offset + paperList.value.limit, paperList.value.total);
  return `${start}-${end} / ${paperList.value.total || 0} · Kafka -> Flink -> MySQL -> WebSocket`;
});
const canPrev = computed(() => paperList.value.offset > 0);
const canNext = computed(() => paperList.value.offset + paperList.value.limit < paperList.value.total);

const yearlyOption = computed(() => ({
  tooltip: { trigger: "axis" },
  grid: { left: 42, right: 18, top: 28, bottom: 36 },
  xAxis: { type: "category", data: yearly.value.map((item) => item.year), axisLabel: { color: "#9aa4b2" } },
  yAxis: { type: "value", axisLabel: { color: "#9aa4b2" }, splitLine: { lineStyle: { color: "#223047" } } },
  series: [{ type: "line", smooth: true, symbolSize: 8, data: yearly.value.map((item) => item.count), areaStyle: {}, lineStyle: { width: 3, color: "#34d399" }, itemStyle: { color: "#38bdf8" } }],
}));

const wordCloudOption = computed(() => ({
  tooltip: {},
  series: [{
    type: "wordCloud",
    sizeRange: [12, 44],
    rotationRange: [-30, 30],
    gridSize: 8,
    textStyle: {
      color: () => ["#38bdf8", "#34d399", "#f8c14a", "#f472b6", "#c4b5fd"][Math.floor(Math.random() * 5)],
    },
    data: keywords.value.map((item) => ({ name: item.name, value: item.value })),
  }],
}));

const authorOption = computed(() => barOption(authors.value, "#38bdf8"));
const institutionOption = computed(() => barOption(institutions.value, "#f8c14a"));

function barOption(items, color) {
  return {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 120, right: 18, top: 20, bottom: 24 },
    xAxis: { type: "value", axisLabel: { color: "#9aa4b2" }, splitLine: { lineStyle: { color: "#223047" } } },
    yAxis: { type: "category", data: items.map((item) => item.name).reverse(), axisLabel: { color: "#cbd5e1", width: 110, overflow: "truncate" } },
    series: [{ type: "bar", data: items.map((item) => item.value).reverse(), itemStyle: { color, borderRadius: [0, 4, 4, 0] } }],
  };
}

async function refresh() {
  const data = await fetchDashboard({
    q: query.value,
    year: year.value,
    limit: paperList.value.limit,
    offset: paperList.value.offset,
  });
  paperList.value = data.paperList;
  papers.value = data.paperList.items || [];
  summary.value = data.summary;
  years.value = data.years;
  yearly.value = data.yearly;
  keywords.value = data.keywords;
  authors.value = data.authors;
  institutions.value = data.institutions;
}

function applyFilters() {
  query.value = draftQuery.value;
  year.value = draftYear.value;
  paperList.value = { ...paperList.value, offset: 0 };
  selectedPaper.value = null;
  refresh();
}

function resetFilters() {
  draftQuery.value = "";
  draftYear.value = "";
  query.value = "";
  year.value = "";
  paperList.value = { ...paperList.value, offset: 0 };
  selectedPaper.value = null;
  refresh();
}

function changePage(direction) {
  const nextOffset = paperList.value.offset + direction * paperList.value.limit;
  paperList.value = { ...paperList.value, offset: Math.max(0, nextOffset) };
  refresh();
}

function connectWebSocket() {
  const url = import.meta.env.VITE_WS_URL || "ws://localhost:8080/api/ws";
  const socket = new WebSocket(url);
  socket.onopen = () => { connected.value = true; };
  socket.onclose = () => {
    connected.value = false;
    setTimeout(connectWebSocket, 3000);
  };
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "paper.updated") {
      refresh();
    }
  };
}

onMounted(() => {
  refresh();
  connectWebSocket();
});
</script>
