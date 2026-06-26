<template>
  <section class="panel">
    <header class="panel__header">
      <h2>{{ title }}</h2>
      <span v-if="meta">{{ meta }}</span>
    </header>
    <div ref="chartEl" class="chart"></div>
  </section>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from "vue";
import * as echarts from "echarts";
import "echarts-wordcloud";

const props = defineProps({
  title: { type: String, required: true },
  meta: { type: String, default: "" },
  option: { type: Object, required: true },
});

const chartEl = ref(null);
let chart;

function render() {
  if (!chart && chartEl.value) {
    chart = echarts.init(chartEl.value);
  }
  if (chart) {
    chart.setOption(props.option, true);
  }
}

function resize() {
  chart?.resize();
}

onMounted(() => {
  render();
  window.addEventListener("resize", resize);
});

watch(() => props.option, render, { deep: true });

onBeforeUnmount(() => {
  window.removeEventListener("resize", resize);
  chart?.dispose();
});
</script>
