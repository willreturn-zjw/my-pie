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
    // 新增字段：记录 KV 依赖链。
    // 例如：["intro_kv_key", "good_kv_key"]
    kv_chain: Vec<String>, 
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    eprintln!("[Debug] Good Agent (Delta Mode) started.");

    let input_json: String = args.value_from_str(["-i", "--input"])?;
    let input: AgentInput = serde_json::from_str(&input_json)?;
    let parent_id = &input.parent_task_ids[0];
    
    let model = get_auto_model();
    let queue = model.create_queue();

    // 1. 读取父节点元数据
    let parent_meta_key = format!("{}_meta", parent_id);
    let meta_json = store_get(&parent_meta_key)
        .ok_or_else(|| anyhow::anyhow!("Parent meta not found"))?;
    let mut parent_meta: AgentMeta = serde_json::from_str(&meta_json)?;

    // 2. 级联加载所有历史 KV 页 (Reconstruct Full Chain)
    // 比如：先加载 Intro 的页，如果 Intro 之前还有祖先，也会在 chain 里
    let mut all_kv_pages: Vec<KvPage> = Vec::new();
    
    // 如果父节点是老版本没有 chain 字段，就 fallback 到直接读 parent_kv
    // 这里为了兼容性，我们构建一个新的 chain
    let mut current_chain = parent_meta.kv_chain.clone();
    eprintln!("[Debug] Loading KV Chain: {:?}", current_chain);
    for key in &current_chain {
        let mut pages = queue.import_kv_pages(key);
        all_kv_pages.append(&mut pages);
    }
    let imported_pages_count = all_kv_pages.len();
    eprintln!("[Debug] Total imported pages: {}", imported_pages_count);

    // 3. 创建上下文
    let mut ctx = Context::from_imported_state(
        &model,
        all_kv_pages,
        parent_meta.token_ids,
        parent_meta.kv_page_last_len,
    );

    // 4. 生成新内容
    ctx.fill_user(&input.prompt);
    let sampler = Sampler::top_k_top_p(0.6, 20, 0.95);
    let stop_cond = max_len(1024).or(ends_with_any(model.eos_tokens()));
    let generated_text = ctx.generate(sampler, stop_cond).await;

    // 5. 【关键】计算增量并保存
    // ctx.kv_pages 现在包含了 [Old Pages ... New Pages]
    // 我们只需要切片取出 New Pages
    let total_pages = ctx.kv_pages.len();
    let new_pages_count = total_pages - imported_pages_count;
    
    eprintln!("[Debug] Total: {}, Imported: {}, New: {}", total_pages, imported_pages_count, new_pages_count);
    
    let my_kv_key = format!("{}_kv", input.task_id);
    
    if new_pages_count > 0 {
        // 提取新生成的页面
        // 注意：这里需要创建一个新的 slice 或者 vec 来导出
        // Rust 的 slice 索引： &ctx.kv_pages[imported_pages_count..]
        let new_pages = &ctx.kv_pages[imported_pages_count..];
        ctx.queue().export_kv_pages(new_pages, &my_kv_key);
        eprintln!("[Debug] Exported {} delta pages to {}", new_pages.len(), my_kv_key);
    } else {
        eprintln!("[Debug] No new full pages generated. (Might only have partial page data in last_len)");
        // 即使没有满页，我们也占位一个空 key 或者在 chain 里复用逻辑？
        // 简单起见，我们假设总会有数据，或者允许空导出
        ctx.queue().export_kv_pages(&[], &my_kv_key);
    }

    // 6. 更新链条并保存 Meta
    current_chain.push(my_kv_key); // 将自己的 KV 加入链条末尾

    let my_meta = AgentMeta {
        token_ids: ctx.get_token_ids().to_vec(),
        kv_page_last_len: ctx.get_kv_page_last_len(),
        kv_chain: current_chain, // 传递给下一代
    };
    
    store_set(&format!("{}_meta", input.task_id), &serde_json::to_string(&my_meta)?);
    store_set(&format!("{}_output", input.task_id), &generated_text);

    eprintln!("[Debug] Saved. Chain length: {}", my_meta.kv_chain.len());
    
    std::mem::forget(ctx);
    Ok(generated_text)
}