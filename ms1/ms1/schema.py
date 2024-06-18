from sqlmodel import Field, Session, SQLModel, create_engine, select


class Order(SQLModel):
    id: int | None = Field(default=None)
    product: str = Field(index=True)
    product_id: int | None = Field(default=None, index=True)