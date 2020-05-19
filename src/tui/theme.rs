use cursive::theme::{BaseColor, Color, PaletteColor, Theme};
use cursive::Cursive;

pub fn create_theme(siv: &Cursive) -> Theme {
    let mut theme = siv.current_theme().clone();

    // theme.palette[PaletteColor::View] = Color::TerminalDefault;
    theme.palette[PaletteColor::View] = Color::Dark(BaseColor::Black);
    theme.palette[PaletteColor::Primary] = Color::Dark(BaseColor::White);
    theme.palette[PaletteColor::Secondary] = Color::Light(BaseColor::Red);
    theme.palette[PaletteColor::Highlight] = Color::Dark(BaseColor::Cyan);

    theme
}
