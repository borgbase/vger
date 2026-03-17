use std::cell::RefCell;

use crossbeam_channel::Sender;
use slint::ComponentHandle;

use crate::messages::{AppCommand, FindResultRow};
use crate::view_models::to_table_model;
use crate::FindWindow;

thread_local! {
    /// Strong reference keeps the window alive even when hidden, so in-flight
    /// FindResultsData events are never dropped.
    static FIND_HANDLE: RefCell<Option<FindWindow>> = const { RefCell::new(None) };
}

/// Return the existing FindWindow or lazily create and wire one.
/// Must be called on the main (UI) thread.
pub(crate) fn ensure_window(app_tx: &Sender<AppCommand>) -> Option<FindWindow> {
    FIND_HANDLE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if let Some(ref fw) = *borrow {
            return Some(fw.clone_strong());
        }
        let fw = FindWindow::new().ok()?;
        wire_callbacks(&fw, app_tx.clone());
        let handle = fw.clone_strong();
        *borrow = Some(fw);
        Some(handle)
    })
}

/// Access the FindWindow if it exists (runs closure on main thread).
pub(crate) fn with_window(f: impl FnOnce(&FindWindow)) {
    FIND_HANDLE.with(|cell| {
        if let Some(ref fw) = *cell.borrow() {
            f(fw);
        }
    });
}

pub(crate) fn handle_results(fw: &FindWindow, rows: Vec<FindResultRow>) {
    let count = rows.len();
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|r| vec![r.snapshot, r.path, r.date, r.size, r.status])
        .collect();
    fw.set_result_rows(to_table_model(table_rows));
    fw.set_status_text(format!("{count} results found.").into());
}

fn wire_callbacks(find_win: &FindWindow, app_tx: Sender<AppCommand>) {
    {
        let tx = app_tx.clone();
        let fw_weak = find_win.as_weak();
        find_win.on_search_clicked(move || {
            let Some(fw) = fw_weak.upgrade() else {
                return;
            };
            let repo = fw.get_repo_combo_value().to_string();
            let pattern = fw.get_name_pattern().to_string();
            if repo.is_empty() || pattern.is_empty() {
                fw.set_status_text("Please select a repository and enter a name pattern.".into());
                return;
            }
            fw.set_status_text("Searching...".into());
            fw.set_result_rows(to_table_model(vec![]));
            let _ = tx.send(AppCommand::FindFiles {
                repo_name: repo,
                name_pattern: pattern,
            });
        });
    }

    {
        let fw_weak = find_win.as_weak();
        find_win.on_close_clicked(move || {
            if let Some(fw) = fw_weak.upgrade() {
                let _ = fw.hide();
            }
        });
    }

    // Titlebar close — hide instead of destroy.
    find_win
        .window()
        .on_close_requested(|| slint::CloseRequestResponse::HideWindow);
}
