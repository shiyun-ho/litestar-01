from contextlib import asynccontextmanager
from typing import Any
from collections.abc import AsyncGenerator, Sequence

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from litestar import Litestar, get, post, put
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException
from litestar.status_codes import HTTP_409_CONFLICT

TodoType = dict[str, Any]
TodoCollectionType = list[TodoType]

class Base(DeclarativeBase): ...

class TodoItem(Base):
    __tablename__ = "todo_items"

    title: Mapped[str] = mapped_column(primary_key=True)
    done: Mapped[bool]

# ===================
# 
# ===================
@asynccontextmanager
async def db_connection(app: Litestar) -> AsyncGenerator[None, None]:
    """
    A context manager for managing db connection lifecycle.
    - Creates a new async engine if doesn't exist
    - Ensures tables are created (based on ORM model)
    - Disposes of engine cleanly when app shuts down

    This fn is added to app's lifespan events so it runs once and block
    shutdown until cleanup is complete
    """
    # Check if engine already exists in app state
    engine = getattr(app.state, "engine", None)

    if engine is None:
        # Create new async SQLite engine if not set
        engine = create_async_engine("sqlite+aiosqlite:///todo.sqlite")
        # Store for later use
        app.state.engine = engine
    
    async with engine.begin() as conn:
        # Base.metadata.create_all: Synchronous ORM method
        # Can't be called directly since we are in async context
        # SO -> Wrapped with conn.run_sync():
        #   - Runs the sync func in background thread in thread pool
        #   - prevents blocking in main event loop
        #   - Lets us use SQLAlchemy ORM features safely in async
        await conn.run_sync(Base.metadata.create_all)

    try:
        # Control yield back to app
        yield
    finally: 
        # Clean up - Dispose async engine properly
        await engine.dispose()
    
sessionmaker = async_sessionmaker(expire_on_commit=False)

def serialize_todo(todo: TodoItem) -> TodoType:
    return {"title": todo.title, "done": todo.done}

async def get_todo_by_title(todo_name: str, session: AsyncSession) -> TodoItem:
    query = select(TodoItem).where(TodoItem.title == todo_name)
    result = await session.execute(query)
    try:
        return result.scalar_one()
    except NoResultFound as e:
        raise NotFoundException(detail=f"TODO {todo_name!r} not found")

async def get_todo_list(done: bool | None, session: AsyncSession) -> Sequence[TodoItem]:
    query = select(TodoItem)
    if done is not None: 
        query = query.where(TodoItem.done.is_(done))
    
    result = await session.execute(query)
    return result.scalars().all()

@get("/")
async def get_list(state: State, done: bool | None = None) -> TodoCollectionType:
    async with sessionmaker(bind=state.engine) as session:
        return [serialize_todo(todo) for todo in await get_todo_list(done, session)]

@post("/")
async def add_item(data: TodoType, state: State) -> TodoType:
    new_todo = TodoItem(title=data["title"], done=data["done"]) 
    async with sessionmaker(bind=state.engine) as session:
        try:
            async with session.begin():
                session.add(new_todo)
        except IntegrityError as e:
            raise ClientException(
                status_code=HTTP_409_CONFLICT,
                detail=f"TODO {new_todo.title!r} already exists",
            ) from e
        
        return serialize_todo(new_todo)

@put("/{item_title:str}")
async def update_item(item_title: str, data: TodoType, state: State) -> TodoType:
    async with sessionmaker(bind=state.engine) as session, session.begin():
        todo_item = await get_todo_by_title(item_title, session)
        todo_item.title = data["title"]
        todo_item.done = data["done"]
    
    return serialize_todo(todo_item)

"""
    lifespan: Hook
        - Deal with long running tasks;
        - Keep context object (e.g. connection) around
"""
app = Litestar([get_list, add_item, update_item], lifespan=[db_connection])