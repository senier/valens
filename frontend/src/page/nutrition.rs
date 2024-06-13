use seed::{prelude::*, *};

use crate::{common, data};

// ------ ------
//     Init
// ------ ------

pub fn init(
    mut url: Url,
    orders: &mut impl Orders<Msg>,
    data_model: &data::Model,
    navbar: &mut crate::Navbar,
) -> Model {
    orders.subscribe(Msg::DataEvent);
    navbar.title = String::from("Nutrition");

    Model {}
}

// ------ ------
//     Model
// ------ ------

pub struct Model {}

// ------ ------
//    Update
// ------ ------

pub enum Msg {
    DataEvent(data::Event),
}

pub fn update(
    msg: Msg,
    model: &mut Model,
    data_model: &data::Model,
    orders: &mut impl Orders<Msg>,
) {
}

// ------ ------
//     View
// ------ ------

pub fn view(model: &Model, data_model: &data::Model) -> Node<Msg> {
    div!["Foo"]
}
