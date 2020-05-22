use std::ops::{Add, AddAssign, Index};
use std::str::FromStr;
use indexmap::IndexMap;

/// a lazy group by.
///
/// Takes an iterator of something that can be indexed (a vec, a struct that impl Index)
///
/// I want to be able to aggregate into array, or to sum on the fly
///
/// Also, I want to be able to use csv records, which would be a Result<T>
///
/// Made to work only with String group by vals. The measure cols can be varied types
///
/// Filtering and such should be done before the groupby.
///
/// TODO can this just be chained onto the end of an iterator chain? With an extension trait?
///
#[must_use] // doesn't do anything without
pub struct GroupBy<I, T>
where
    I: Iterator<Item = T>,
    T: Index<usize>,
{
    iter: I,
    group_by_cols: Vec<usize>,
    value_col: usize, // just one for now.
}

/// Initializing.
/// Implementing for generic T::Output seems fine?
impl<I, T> GroupBy<I, T>
where
    I: Iterator<Item = T>,
    T: Index<usize>,
{
    pub fn new(iter: I, group_by_cols: Vec<usize>, value_col: usize) -> Self
    {
        Self {
            iter,
            group_by_cols,
            value_col,
        }
    }
}


/// A bit ugly, but we need to make sure that the key can be converted into a string, and that the
/// value can be converted into a string and then back to a number.
///
/// I'm guessing that when it's monomorphized, and if you're using a String for T::Output, this
/// will all be zero-cost?
impl<I, T> GroupBy<I, T>
where
    I: Iterator<Item = T>,
    T: Index<usize>,
    T::Output: Add + AddAssign + Default + FromStr + std::fmt::Display,
    <<T as Index<usize>>::Output as FromStr>::Err: std::fmt::Debug,
{
    pub fn sum(self) -> Aggregate<T::Output>
    {
        let mut res = IndexMap::new();

        for row in self.iter {
            let key = self.group_by_cols.iter().map(|col| row[*col].to_string()).collect();
            let entry = res.entry(key).or_default();
            *entry += row[self.value_col].to_string().parse().unwrap();
        }

        Aggregate { inner: res }
    }
}

/// Implementing for a different T::Output is fine
/// TODO let's try to make this work for something where Aggregate<V> V is a Vec
impl<I, T> GroupBy<I, T>
where
    I: Iterator<Item = T>,
    T: Index<usize>,
    T::Output: std::ops::Mul + std::ops::MulAssign + Default + FromStr + std::fmt::Display,
    <<T as Index<usize>>::Output as FromStr>::Err: std::fmt::Debug,
{
    pub fn group_array(self) -> Aggregate<Vec<T::Output>> {
        Aggregate { inner: IndexMap::new() }
    }
}

pub struct Aggregate<V> {
    inner: IndexMap<GroupKey, V>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}

pub type GroupKey = Vec<String>;

