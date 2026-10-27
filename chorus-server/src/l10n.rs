#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Lang {
    #[default]
    EnUs,
    ZhCn,
    ZhTw,
}

#[derive(Clone, Copy, Debug)]
pub enum Text {
    RoomIdTaken,
    GameInProgress,
    RoomFull,
    RoomLocked,
    CannotMonitor,
    NoChartSelected,
    LoggedInElsewhere,
}

impl Lang {
    pub fn from_tag(tag: &str) -> Self {
        let tag = tag.to_ascii_lowercase().replace('_', "-");
        match tag.as_str() {
            "zh-cn" | "zh-sg" | "zh-hans" | "zh-hans-cn" | "zh" => Self::ZhCn,
            "zh-tw" | "zh-hk" | "zh-mo" | "zh-hant" | "zh-hant-tw" | "zh-hant-hk" => Self::ZhTw,
            _ => Self::EnUs,
        }
    }

    pub fn text(self, text: Text) -> &'static str {
        use Text::*;
        match self {
            Self::EnUs => match text {
                RoomIdTaken => "Room ID is occupied",
                GameInProgress => "Game is ongoing",
                RoomFull => "Room is full",
                RoomLocked => "Room is locked",
                CannotMonitor => "Permission denied. You can't monitor this room.",
                NoChartSelected => "No chart selected",
                LoggedInElsewhere => "Your account is now connected from another session",
            },
            Self::ZhCn => match text {
                RoomIdTaken => "房间 ID 已被占用",
                GameInProgress => "游戏正在进行中",
                RoomFull => "房间已满",
                RoomLocked => "房间已锁定",
                CannotMonitor => "权限不足，不能旁观房间",
                NoChartSelected => "还没有选择谱面",
                LoggedInElsewhere => "你的账号已在其他连接上登录",
            },
            Self::ZhTw => match text {
                RoomIdTaken => "房間 ID 已被佔用",
                GameInProgress => "遊戲正在進行中",
                RoomFull => "房間已滿",
                RoomLocked => "房間已鎖定",
                CannotMonitor => "權限不足，不能旁觀房間",
                NoChartSelected => "還沒有選擇譜面",
                LoggedInElsewhere => "你的帳號已在其他連線上登入",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_languages_fall_back_to_english() {
        assert_eq!(Lang::from_tag("zh-CN"), Lang::ZhCn);
        assert_eq!(Lang::from_tag("zh-TW"), Lang::ZhTw);
        assert_eq!(Lang::from_tag("zh_Hant"), Lang::ZhTw);
        assert_eq!(Lang::from_tag("en-US"), Lang::EnUs);
        assert_eq!(Lang::from_tag("ja-JP"), Lang::EnUs);
        assert_eq!(Lang::from_tag(""), Lang::EnUs);
    }
}
