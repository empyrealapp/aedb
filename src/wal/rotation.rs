#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReason {
    Size,
    Time,
    Forced,
}
