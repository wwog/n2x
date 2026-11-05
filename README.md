# n2x

**n2x — N-to-X Data Migration Framework**

n2x 是一个 Rust 编写的通用数据迁移框架，用统一抽象描述「从任意源 N 到任意目标 X」的迁移。

## 设计原则

1. **配置即计划**：由 manifest 做迁移唯一依据。



标准化

```

plan {
    v0 -> 见数据库  Mode::Model {out:}
    v0 -> 初始化缓存目录  Mode::Dir {} 
}

plan {
    v1 -> 文件结构调整 Mode::Dir {}
    v1 -> db -> file Mode::Model {}
}


plan {
    v2 -> 删除文件
}

plan {
    v0 ->
    v3
}
scheduler.after
create table,
create table_index
scheduler.

```

[ version , schema_task {
    flag_user: 100,
    flag_user_done
}]

scheduler.run(plan)