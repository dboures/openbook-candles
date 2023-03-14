use chrono::Duration;
use std::fmt;
use strum::EnumIter;

#[derive(EnumIter, Copy, Clone, Eq, PartialEq)]
pub enum Resolution {
    R1m,
    R3m,
    R5m,
    R15m,
    R30m,
    R1h,
    R2h,
    R4h,
    R1d,
}

pub fn day() -> Duration {
    Duration::days(1)
}

impl fmt::Display for Resolution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Resolution::R1m => write!(f, "1M"),
            Resolution::R3m => write!(f, "3M"),
            Resolution::R5m => write!(f, "5M"),
            Resolution::R15m => write!(f, "15M"),
            Resolution::R30m => write!(f, "30M"),
            Resolution::R1h => write!(f, "1H"),
            Resolution::R2h => write!(f, "2H"),
            Resolution::R4h => write!(f, "4H"),
            Resolution::R1d => write!(f, "1D"),
        }
    }
}

impl Resolution {
    pub fn get_constituent_resolution(self) -> Resolution {
        match self {
            Resolution::R1m => panic!("have to use fills to make 1M candles"),
            Resolution::R3m => Resolution::R1m,
            Resolution::R5m => Resolution::R1m,
            Resolution::R15m => Resolution::R5m,
            Resolution::R30m => Resolution::R15m,
            Resolution::R1h => Resolution::R30m,
            Resolution::R2h => Resolution::R1h,
            Resolution::R4h => Resolution::R2h,
            Resolution::R1d => Resolution::R4h,
        }
    }

    pub fn get_duration(self) -> Duration {
        match self {
            Resolution::R1m => Duration::minutes(1),
            Resolution::R3m => Duration::minutes(3),
            Resolution::R5m => Duration::minutes(5),
            Resolution::R15m => Duration::minutes(15),
            Resolution::R30m => Duration::minutes(30),
            Resolution::R1h => Duration::hours(1),
            Resolution::R2h => Duration::hours(2),
            Resolution::R4h => Duration::hours(4),
            Resolution::R1d => day(),
        }
    }

    pub fn from_str(v: &str) -> Result<Self, ()> {
        match v {
            "1M" => Ok(Resolution::R1m),
            "3M" => Ok(Resolution::R3m),
            "5M" => Ok(Resolution::R5m),
            "15M" => Ok(Resolution::R15m),
            "30M" => Ok(Resolution::R30m),
            "1H" => Ok(Resolution::R1h),
            "2H" => Ok(Resolution::R2h),
            "4H" => Ok(Resolution::R4h),
            "D" => Ok(Resolution::R1d),
            _ => Err(()),
        }
    }
}
