mod screen;


struct MachineState {
    logs: String,
    buffers: String,
    identity: String,
    cluster: String
}


fn main() {

    let screen = screen::Screen::new();

    screen.start();
}
