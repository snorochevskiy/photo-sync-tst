#[macro_export]
macro_rules! img {
    ( $( $x:expr ),* ) => { vec![$( $x, )*] };
}

#[macro_export]
macro_rules! peers {
    ( $( $x:expr ),* ) => { vec![$( vec![$x], )*] };
}
