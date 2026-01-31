use inferlet::{
    sampler::Sampler,
    stop_condition::{max_len, ends_with_any, StopCondition},
    Args, Result, main, get_auto_model, broadcast, subscribe
};
use serde::{Deserialize};
use std::{thread, time::Duration};

#[derive(Debug, Deserialize)]
struct AgentInput {
    prompt: String,
}

async fn generate_text(prompt: &str, max_tokens: usize) -> Result<String> {
    let model = get_auto_model();
    let mut ctx = model.create_context();
    ctx.fill_user(prompt);
    let sampler = Sampler::top_k_top_p(0.6, 20, 0.95);
    let stop_cond = max_len(max_tokens).or(ends_with_any(model.eos_tokens()));
    let output = ctx.generate(sampler, stop_cond).await;
    std::mem::forget(ctx);
    Ok(output)
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    let input_json: String = args.value_from_str(["-i", "--input"])?;
    let input: AgentInput = serde_json::from_str(&input_json)?;
    let instruction = &input.prompt;

    let mut final_output = String::new();

    if instruction.contains("ROLE: WIRE") {
        eprintln!("[Wire] Generating news...");
        let news = generate_text(instruction, 1024).await?;
        
        // 稍作停顿，确保大家都连上了
        thread::sleep(Duration::from_secs(2));
        
        eprintln!("[Wire] Broadcasting...");
        broadcast("topic/global_news", &news);
        final_output = news;

    } else if instruction.contains("ROLE: DESK") {
        eprintln!("[Desk] Subscribing to global wire...");
        let raw_news = subscribe("topic/global_news").await;
        eprintln!("[Desk] Received. Processing...");

        let desk_prompt = format!("SOURCE:\n{}\n\nTASK:\n{}", raw_news, instruction);
        let analysis = generate_text(&desk_prompt, 300).await?;

        // === 关键修改 1：统一发送到 editor_inbox，并加前缀区分 ===
        let (prefix, _color) = if instruction.contains("POLITICS") { ("POLITICS", "red") }
                         else if instruction.contains("TECH") { ("TECH", "blue") }
                         else { ("SPORTS", "green") };

        // 格式： "TAG: Content"
        let payload = format!("{}: {}", prefix, analysis);
        
        eprintln!("[Desk] Sending {} report to 'topic/editor_inbox'...", prefix);
        // 所有 Desk 发送给同一个 Topic，这样 Host 会帮 Editor 缓存消息队列
        broadcast("topic/editor_inbox", &payload);
        
        final_output = analysis;

    } else if instruction.contains("ROLE: EDITOR") {
        eprintln!("[Editor] Waiting for 3 reports in 'topic/editor_inbox'...");
        
        let mut p_report = String::from("(Missing Politics)");
        let mut t_report = String::from("(Missing Tech)");
        let mut s_report = String::from("(Missing Sports)");

        // === 关键修改 2：在一个循环中接收 3 条消息 ===
        // 因为都在同一个 Topic，底层 mpsc queue 会保证消息不丢失，依次取出来
        for i in 1..=3 {
            eprintln!("[Editor] Fetching message {}/3...", i);
            let msg = subscribe("topic/editor_inbox").await;
            
            if msg.starts_with("POLITICS:") {
                eprintln!("[Editor] Got Politics.");
                p_report = msg.replace("POLITICS: ", "");
            } else if msg.starts_with("TECH:") {
                eprintln!("[Editor] Got Tech.");
                t_report = msg.replace("TECH: ", "");
            } else if msg.starts_with("SPORTS:") {
                eprintln!("[Editor] Got Sports.");
                s_report = msg.replace("SPORTS: ", "");
            }
        }

        eprintln!("[Editor] All reports received. Aggregating...");
        let editor_prompt = format!(
            "Combine these:\n[POLITICS]\n{}\n\n[TECH]\n{}\n\n[SPORTS]\n{}\n\nINSTRUCTION:\n{}", 
            p_report, t_report, s_report, instruction
        );

        final_output = generate_text(&editor_prompt, 600).await?;
    }

    Ok(final_output)
}