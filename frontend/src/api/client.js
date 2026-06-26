import axios from "axios";

const baseURL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8080/api";

export const api = axios.create({ baseURL, timeout: 10000 });

export async function fetchDashboard(filters = {}) {
  const paperParams = {
    limit: filters.limit || 12,
    offset: filters.offset || 0,
  };
  if (filters.q) {
    paperParams.q = filters.q;
  }
  if (filters.year) {
    paperParams.year = filters.year;
  }

  const [papers, summary, years, yearly, keywords, authors, institutions] = await Promise.all([
    api.get("/papers", { params: paperParams }),
    api.get("/stats/summary"),
    api.get("/stats/years"),
    api.get("/stats/yearly"),
    api.get("/stats/keywords", { params: { limit: 80 } }),
    api.get("/stats/authors", { params: { limit: 10 } }),
    api.get("/stats/institutions", { params: { limit: 10 } }),
  ]);
  return {
    paperList: papers.data.data || { items: [], total: 0, limit: paperParams.limit, offset: paperParams.offset },
    summary: summary.data.data || {},
    years: years.data.data || [],
    yearly: yearly.data.data || [],
    keywords: keywords.data.data || [],
    authors: authors.data.data || [],
    institutions: institutions.data.data || [],
  };
}
