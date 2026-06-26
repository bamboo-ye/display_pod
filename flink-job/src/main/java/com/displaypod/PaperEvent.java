package com.displaypod;

import java.util.ArrayList;
import java.util.List;

public class PaperEvent {
    public String source_id;
    public int year;
    public String title;
    public List<String> authors = new ArrayList<>();
    public List<String> institutions = new ArrayList<>();
    public String abstractText;
    public List<String> keywords = new ArrayList<>();
    public String paper_url;
    public String pdf_url;
    public String crawled_at;

    public String getAbstractText() {
        return abstractText;
    }
}
