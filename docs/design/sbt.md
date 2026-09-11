# sbt 的原则与实践

构建配置要让维护者在模块声明附近看懂它依赖什么、如何编译和测试、是否发布；公共规则的影响范围应当由名字和调用位置说明。

本页把 [AGENTS.md](../../AGENTS.md) 的整体一致性原则落实到 sbt。[modules.md](modules.md) 规定模块边界和兼容矩阵，本页规定如何表达这些约束。修改构建时使用仓库内的 [spark-milvus-sbt skill](../../.agents/skills/spark-milvus-sbt/SKILL.md)。

## 1 以阅读成本判断设计

**关键行为就近声明。** 模块的代码依赖、是否发布、测试是否需要外部服务，应当在模块处可见。具体实现可以展开阅读，但不应靠逐层打开公共设置才发现一个模块被禁止发布。

**共享同一种规则。** 四条 Spark 线共用源码和构建方式，适合一个按线工厂。apps 和 integration 各只有一个实例，直接声明具体项目更容易阅读。若多个调用者应当一起接受规则变化，才有抽取的理由；少量重复可以保留。

**名字说明效果。** Scala/Java 编译设置不应顺带改变发布策略。一个明确叫作 `sparkFreeModuleSettings` 的设置可以同时带入跨线编译基线、通用依赖和禁止 Spark 引用的检查，因为这些共同定义了第二层模块；发布开关仍写在使用处。

**文件划分服务于理解。** 大部分配置留在 `build.sbt`，公共值和构建实现可放在 `project/*.scala`，这是 [sbt 官方建议](https://www.scala-sbt.org/1.x/docs/Organizing-Build.html)。集中维护的价值在于一致性；一处使用的版本或坐标可以就地声明，不要求每个字面量都经过两层转发。既有文件的调整以实际阅读收益为依据。

**说明当前理由。** 注释解释 scope、运行时约束和例外的退出条件。历史讨论进入 [决策日志](README.md#6-决策日志)。缩短文件、减少重复行、增加注释都不能单独证明构建更容易理解。

## 2 三种关系分别表达

`.aggregate` 表示运行任务时覆盖哪些项目，`.dependsOn` 表示使用哪些项目的代码，`.settings` 表示应用哪些配置。聚合不会传递代码或公共配置。共享默认值由 `ThisBuild` 提供，项目设置可以覆盖它；root 是普通项目，不是其他项目的配置父类。见 [多项目构建](https://www.scala-sbt.org/1.x/docs/Multi-Project.html) 和 [作用域](https://www.scala-sbt.org/1.x/docs/Scopes.html)。

`Compile`、`Test` 和具体 task 的 scope 要按实际用途写出。复制设置时先确认它原来属于哪个项目与 scope，尤其是 `fullClasspath`、`javaOptions`、`publish` 和 `assembly`。共享设置需要使用当前项目的目录时，在应用该设置的项目里求值；仓库级输入才取 `ThisBuild / baseDirectory`。

## 3 模块声明的写法

第二层模块把边界约束和发布策略分别写出，例如：

```scala
lazy val compat = Project("compat", file("compat"))
  .dependsOn(core)
  .settings(
    name := "compat",
    moduleName := "spark-milvus-compat",
    Modules.sparkFreeModuleSettings,
    Modules.jacksonPin,
    publish / skip := true,
    libraryDependencies ++= Seq(/* this module's dependencies */)
  )
```

Spark 的四个项目保留显式名称，通过一个 `sparkProject` 工厂应用同一规则。工厂定义里显示代码依赖、共享源码目录、发布策略和依赖组合。增加一条线时，同时加入版本矩阵、项目声明和 root 聚合列表；显式登记便于核对覆盖范围。

apps 和 integration 使用具体的 `Project` 声明，目录、代码依赖和所属 Spark 线就近可见。integration 使用普通 `Test` 配置，并在 root 的聚合列表之外；调用 `integration40/Test/compile` 编译，调用 `integration40/test` 才运行需要 Milvus、MinIO 的测试。

共享源码的路径直接写在对应源码设置旁边；单纯拼一次路径不需要一个包装函数。JNI JAR 路径和测试 JVM 选项会被多个项目共同使用，保留有明确用途的设置片段。

## 4 版本与依赖的写法

Spark、Scala、Java、Arrow 的兼容矩阵在 `Versions` 中统一维护，具体取值遵循 [模块约束](modules.md#4-构建约束)。修改一项时核对同一行的运行环境，不能用另一连接器的配置代替 Spark 发行版的实际依赖。

`Dependencies` 集中共享坐标和需要一起维护的依赖组合；模块特有的依赖可以直接写在模块声明中。组合的名字要说明用途，不能仅因两个模块碰巧有相同的列表，就把它们绑定为同一种规则。

依赖声明要能解释三件事：编译用什么、运行时由谁提供、是否进入交付的 JAR。`provided` 和 `Test` 属于契约。Jackson 对齐只用于 core、compat、client，Spark 线使用自身的一组运行时依赖。整理配置时保留已有的版本、scope 和 exclusions，版本升级单独说明原因和验证依据。

## 5 root 的交付职责

root 在模块声明处明确承担仓库任务聚合和迁移期的 assembly 交付；主 Maven JAR 绑定到 assembly，assembly 与发布都不向子项目聚合。子模块是否发布在自己的声明处可见。

root 的运行参数、assembly 规则、发布元数据分别使用 `rootRunSettings`、`rootAssemblySettings`、`rootPublishingSettings` 三个命名片段，并在 root 声明处应用。片段保留在同一份 `build.sbt` 中，维护者可以按职责展开，不引入额外的项目、插件或配置框架。片段内的设置继承应用处的项目作用域，避免在文件尾部继续追加离散的 `root / ...` 覆盖。

迁移期仍保留以下交付条件：

- 根产物沿用现有 `spark-connector` 坐标、带分支与架构的版本，以及 Docker 查找的 assembly 文件名。分线发布契约和下游迁移验证就绪后，再替换这一交付入口。
- `legacyRootDeps` 保留旧根产物的依赖声明；`legacyRootArrow` 只说明该声明的来源，最终解析版本还受依赖图影响。它不作为 Spark 线的 Arrow 基线。
- 上游 Scala 2.13 JNI JAR 由 `legacyJni` 引入。native-storage 替换该绑定后解除这一临时依赖；此前不宣称 Spark 3.5 的 Scala 2.12 产物已可用。
- Maven POM 排除已嵌入、尚未独立发布的模块依赖，外部依赖保留。JAR 内容和依赖元数据分别核对；Maven POM 的处理不能证明本地 Ivy 发布也正确。

## 6 如何检查一次整改

先固定当前提交和工作区差异，再比较修改前后的项目 ID、目录、`dependsOn`、聚合成员、依赖版本与 scope、测试输入以及交付坐标。多人同时工作时以固定基线核对，保留期间出现的新提交和无关改动。

运行检查按任务授权选择。日常整理默认由 CI 编译和测试，本地可以做静态对照与 `git diff --check`；若执行了 `show`、`inspect`、`makePom` 或编译测试，应分别记录它们验证了什么。静态等价、构建加载、Maven 解析和实际 Spark 运行是不同层次的证据。

审查时用具体维护动作检验可读性：增加一条 Spark 线、修改一个模块的依赖、判断一个项目是否发布。若仍需追踪多个含糊的 helper 才能作答，应先修正这些关系的表达。发布入口、能力、公共类型和测试的删除遵循 AGENTS.md 的说明与授权要求。
