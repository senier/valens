use chrono::prelude::*;
use seed::{prelude::*, *};

use crate::common;

// ------ ------
//     Init
// ------ ------

pub fn init(url: Url, orders: &mut impl Orders<Msg>, session: crate::Session) -> Model {
    let base_url = url.to_hash_base_url();

    orders
        .send_msg(Msg::FetchBodyWeight)
        .send_msg(Msg::FetchBodyFat);

    Model {
        base_url,
        session,
        body_weight: None,
        body_fat: None,
        errors: Vec::new(),
    }
}

// ------ ------
//     Model
// ------ ------

pub struct Model {
    base_url: Url,
    session: crate::Session,
    body_weight: Option<BodyWeight>,
    body_fat: Option<BodyFat>,
    errors: Vec<String>,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct BodyWeight {
    date: NaiveDate,
    weight: f32,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct BodyFat {
    date: NaiveDate,
    #[allow(dead_code)]
    chest: u8,
    #[allow(dead_code)]
    abdominal: u8,
    #[allow(dead_code)]
    tigh: u8,
    #[allow(dead_code)]
    tricep: u8,
    #[allow(dead_code)]
    subscapular: u8,
    #[allow(dead_code)]
    suprailiac: u8,
    #[allow(dead_code)]
    midaxillary: u8,
    jp3: f32,
    #[allow(dead_code)]
    jp7: f32,
}

// ------ ------
//    Update
// ------ ------

pub enum Msg {
    CloseErrorDialog,

    FetchBodyWeight,
    BodyWeightFetched(Result<Vec<BodyWeight>, String>),

    FetchBodyFat,
    BodyFatFetched(Result<Vec<BodyFat>, String>),
}

pub fn update(msg: Msg, model: &mut Model, orders: &mut impl Orders<Msg>) {
    match msg {
        Msg::CloseErrorDialog => {
            model.errors.remove(0);
        }

        Msg::FetchBodyWeight => {
            orders.skip().perform_cmd(async {
                common::fetch("api/body_weight", Msg::BodyWeightFetched).await
            });
        }
        Msg::BodyWeightFetched(Ok(body_weight)) => {
            model.body_weight = body_weight.last().cloned();
        }
        Msg::BodyWeightFetched(Err(message)) => {
            model
                .errors
                .push("Failed to fetch body weight: ".to_owned() + &message);
        }

        Msg::FetchBodyFat => {
            orders
                .skip()
                .perform_cmd(async { common::fetch("api/body_fat", Msg::BodyFatFetched).await });
        }
        Msg::BodyFatFetched(Ok(body_fat)) => {
            model.body_fat = body_fat.last().cloned();
        }
        Msg::BodyFatFetched(Err(message)) => {
            model
                .errors
                .push("Failed to fetch body fat: ".to_owned() + &message);
        }
    }
}

// ------ ------
//     View
// ------ ------

pub fn view(model: &Model) -> Node<Msg> {
    let local: NaiveDate = Local::now().date().naive_local();
    let body_weight_subtitle;
    let body_weight_content;
    let body_fat_subtitle;
    let body_fat_content;

    if let Some(body_weight) = &model.body_weight {
        body_weight_subtitle = format!("{:.1} kg", body_weight.weight);
        body_weight_content = last_update(local - body_weight.date);
    } else {
        body_weight_subtitle = String::new();
        body_weight_content = String::new();
    }

    if let Some(body_fat) = &model.body_fat {
        body_fat_subtitle = format!("{:.1} %", body_fat.jp3);
        body_fat_content = last_update(local - body_fat.date);
    } else {
        body_fat_subtitle = String::new();
        body_fat_content = String::new();
    }

    div![
        common::view_error_dialog(&model.errors, &ev(Ev::Click, |_| Msg::CloseErrorDialog)),
        view_tile(
            "Workouts",
            "",
            "",
            crate::Urls::new(&model.base_url).workouts()
        ),
        view_tile(
            "Routines",
            "",
            "",
            crate::Urls::new(&model.base_url).routines()
        ),
        view_tile(
            "Exercises",
            "",
            "",
            crate::Urls::new(&model.base_url).exercises()
        ),
        view_tile(
            "Body weight",
            &body_weight_subtitle,
            &body_weight_content,
            crate::Urls::new(&model.base_url).body_weight()
        ),
        view_tile(
            "Body fat",
            &body_fat_subtitle,
            &body_fat_content,
            crate::Urls::new(&model.base_url).body_fat()
        ),
        IF![
            model.session.sex == 0 => {
                view_tile("Period", "", "", crate::Urls::new(&model.base_url).period())
            }
        ],
    ]
}

fn view_tile(title: &str, subtitle: &str, content: &str, target: Url) -> Node<Msg> {
    div![
        C!["tile"],
        C!["is-ancestor"],
        C!["is-vertical"],
        C!["mx-0"],
        div![
            C!["tile"],
            C!["is-parent"],
            div![
                C!["tile"],
                C!["is-child"],
                C!["box"],
                div![
                    C!["is-flex"],
                    C!["is-justify-content-space-between"],
                    div![a![
                        C!["title"],
                        C!["is-size-4"],
                        C!["has-text-link"],
                        attrs! {
                            At::Href => target,
                        },
                        title,
                    ]],
                    div![a![
                        C!["title"],
                        C!["is-size-4"],
                        C!["has-text-link"],
                        attrs! {
                            At::Href => target.add_hash_path_part("add"),
                        },
                        span![C!["icon"], i![C!["fas fa-plus-circle"]]]
                    ]],
                ],
                IF![!subtitle.is_empty() => p![C!["subtitle"], C!["is-size-5"], subtitle]],
                IF![!content.is_empty() => p![C!["content"], raw![content]]]
            ],
        ],
    ]
}

fn last_update(duration: chrono::Duration) -> String {
    if duration.num_days() == 0 {
        return String::from("Last update <strong>today</strong>.");
    }

    if duration.num_days() == 1 {
        return String::from("Last update <strong>yesterday</strong>.");
    }

    format!(
        "Last update <strong>{} days</strong> ago.",
        duration.num_days()
    )
}