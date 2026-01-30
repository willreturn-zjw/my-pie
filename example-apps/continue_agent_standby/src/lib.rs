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
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    eprintln!("[Debug] Bad Agent (Text-Only Mode) started.");

    // 1. 解析输入
    let input_json: String = args.value_from_str(["-i", "--input"])?;
    let input: AgentInput = serde_json::from_str(&input_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse input JSON: {}", e))?;

    if input.parent_task_ids.is_empty() {
        anyhow::bail!("Agent requires a parent task ID.");
    }
    let parent_id = &input.parent_task_ids[0];
    
    // 2. 准备环境 (不创建 Queue，不导入 KV)
    let model = get_auto_model();
    let mut ctx = model.create_context();

    // 3. 【关键差异】读取父节点的“纯文本输出”
    let parent_output_key = format!("{}_output", parent_id);
    eprintln!("[Debug] Fetching parent text from: {}", parent_output_key);
    
    let parent_text = store_get(&parent_output_key)
        .ok_or_else(|| anyhow::anyhow!("Parent output text not found: {}", parent_id))?;
    
    eprintln!("[Debug] Loaded parent text (len: {}). Re-computing prefill...", parent_text.len());

    // 4. 重建上下文 (手动拼接历史)
    // 这样，node_bad 就拥有了自己独立的显存，完全不依赖 intro 遗留的显存指针
    ctx.fill_system("You are a fantasy novel writer."); 
    ctx.fill_user(&format!("{}\n\n{}", parent_text, input.prompt));

    // 5. 执行推理
    let sampler = Sampler::top_k_top_p(0.6, 20, 0.95);
    let stop_cond = max_len(1024).or(ends_with_any(model.eos_tokens()));
    
    let generated_text = ctx.generate(sampler, stop_cond).await;
    eprintln!("[Debug] Generation complete. Length: {}", generated_text.len());

    // 6. 保存状态
    let my_kv_key = format!("{}_kv", input.task_id);
    // 这里的 export 是安全的，因为这是 node_bad 私有的显存
    ctx.queue().export_kv_pages(&ctx.kv_pages, &my_kv_key);

    let my_meta = AgentMeta {
        token_ids: ctx.get_token_ids().to_vec(),
        kv_page_last_len: ctx.get_kv_page_last_len(),
    };
    store_set(&format!("{}_meta", input.task_id), &serde_json::to_string(&my_meta)?);
    store_set(&format!("{}_output", input.task_id), &generated_text);

    eprintln!("[Debug] State saved. Normal exit.");
    
    // 这里依然推荐 forget，这是良好的习惯，防止清理自己还要用的显存
    std::mem::forget(ctx);

    Ok(generated_text)
}