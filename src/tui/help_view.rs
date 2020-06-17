use cursive::theme::Effect;
use cursive::views::{DummyView, LinearLayout, ResizedView, TextView};
use cursive::Cursive;

struct KeyCommand {
    key: String,
    description: String,
}

lazy_static! {
    static ref COMMANDS: Vec<KeyCommand> = vec!(
        KeyCommand {
            key: "?".to_string(),
            description: "Display this screen".to_string()
        },
        KeyCommand {
            key: "q".to_string(),
            description: "Close the current screen".to_string()
        },
        KeyCommand {
            key: "/".to_string(),
            description: "Fuzzy search for a topic".to_string()
        },
    );
}

pub fn render_help_view(siv: &mut Cursive) {
    siv.add_fullscreen_layer(ResizedView::with_full_screen(
        LinearLayout::horizontal()
            .child(ResizedView::with_full_width(DummyView))
            .child(
                LinearLayout::vertical()
                    .child(TextView::new("Help").effect(Effect::Bold))
                    .child(DummyView)
                    .child(TextView::new(format_commands())),
            )
            .child(ResizedView::with_full_width(DummyView)),
    ));
}

fn format_commands() -> String {
    let lines: Vec<String> = COMMANDS
        .iter()
        .map(|command| format!("{} {}", command.key, command.description))
        .collect();

    lines.join("\n")
}
