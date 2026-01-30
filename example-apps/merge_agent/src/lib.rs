use inferlet::{
    forward::{Forward, KvPage},
    sampler::Sampler,
    stop_condition::{max_len, ends_with_any, StopCondition},
    Args, Queue, Result, Tokenizer, main, get_auto_model, store_set, store_get, Context
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct AgentInput {
    task_id: String,
    parent_task_ids: Vec<String>,
    prompt: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentMeta {
    token_ids: Vec<u32>,
    kv_page_last_len: usize,
    // Finale 也会读取这个链条
    #[serde(default)] 
    kv_chain: Vec<String>,
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    eprintln!("[Debug] Finale Agent (Chain-KV Mode) started.");

    let input_json: String = args.value_from_str(["-i", "--input"])?;
    let input: AgentInput = serde_json::from_str(&input_json)?;
    
    // 约定：
    // parent_task_ids[0] 是 Base (提供 KV 基础)
    // parent_task_ids[1..] 都是 Reference (提供纯文本素材)
    if input.parent_task_ids.len() < 2 {
        anyhow::bail!("Merge Agent requires at least one base and one reference.");
    }
    
    let base_id = &input.parent_task_ids[0];
    
    // 1. 动态加载所有参考分支的文本
    let mut references_text = String::new();
    for (idx, ref_id) in input.parent_task_ids.iter().skip(1).enumerate() {
        let ref_key = format!("{}_output", ref_id);
        let text = store_get(&ref_key).unwrap_or_else(|| "[(Missing Data)]".to_string());
        
        // 格式化拼接到 prompt 中
        use std::fmt::Write;
        write!(references_text, "\n=== Perspective {} (Source: {}) ===\n{}\n", idx + 1, ref_id, text).ok();
    }

    eprintln!("[Debug] Loaded {} reference texts.", input.parent_task_ids.len() - 1);

    // 2. 加载 Base 的元数据 (和之前一样)
    let base_meta_key = format!("{}_meta", base_id);
    let meta_json = store_get(&base_meta_key)
        .ok_or_else(|| anyhow::anyhow!("Base meta not found"))?;
    let mut meta: AgentMeta = serde_json::from_str(&meta_json)?;

    // 3. 重建 KV 链条 (The Chain of Memory)
    let model = get_auto_model();
    let queue = model.create_queue();
    
    let mut all_kv_pages: Vec<KvPage> = Vec::new();
    
    // 如果上游 Good 成功生成了 chain，我们就用 chain
    // 如果是旧代码遗留，我们做个兼容
    let mut load_list = meta.kv_chain.clone();
    if load_list.is_empty() {
         // Fallback logic
         load_list.push(format!("{}_kv", base_id));
    }

    eprintln!("[Debug] Reconstructing memory from chain: {:?}", load_list);
    for key in &load_list {
        let mut pages = queue.import_kv_pages(key);
        eprintln!("[Debug]  -> Loaded {} pages from {}", pages.len(), key);
        all_kv_pages.append(&mut pages);
    }

    // 4. 恢复上下文
    let mut ctx = Context::from_imported_state(
        &model,
        all_kv_pages,
        meta.token_ids,
        meta.kv_page_last_len,
    );

    // 5. 混合 Prompt
    let hybrid_prompt = format!(
        "The following are different accounts of the same event from different perspectives:\n{}\n\nBased on the main timeline (Perspective 1) and the conflicting accounts above, analyze the truth and summarize what truly happened: {}", 
        references_text, 
        input.prompt
    );
    ctx.fill_user(&hybrid_prompt);

    // 6. 生成
    let sampler = Sampler::top_k_top_p(0.6, 20, 0.95);
    let stop_cond = max_len(1024).or(ends_with_any(model.eos_tokens()));
    
    eprintln!("[Debug] Generating Finale...");
    let generated_text = ctx.generate(sampler, stop_cond).await;
    eprintln!("[Debug] Finale Length: {}", generated_text.len());

    store_set(&format!("{}_output", input.task_id), &generated_text);

    // 7. 甚至 Finale 也可以继续导出增量，形成第 4 轮...
    // 代码逻辑同 Good，略。
    
    std::mem::forget(ctx);
    Ok(generated_text)
}