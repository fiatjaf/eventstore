module events {
    type Event {
        required eventId: str {
            constraint exclusive;
        };
        required pubkey: str;
        required createdAt: datetime;
        required kind: int64;
        tags: json;
        content: str;
        required sig: str {
            constraint exclusive;
        };
    }
}