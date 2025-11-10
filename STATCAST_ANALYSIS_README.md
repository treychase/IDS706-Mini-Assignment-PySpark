# Statcast Baseball Data Analysis (2017-2021)

## Overview

This notebook (`statcast_analysis.ipynb`) provides a comprehensive PySpark analysis pipeline for 5 years of MLB Statcast pitch-level baseball data. It demonstrates advanced data processing techniques including filtering, joins, aggregations, SQL optimization, performance analysis, caching, and machine learning.

## Data

**Source Files**: `data/statcast_*.csv` (5 years: 2017-2021)
- **Total Records**: ~3.15 million pitches
- **Columns**: 93 features per pitch (pitch characteristics, batter/pitcher info, outcomes, etc.)
- **Combined Size**: ~360 MB CSV data

### Statcast Sample Columns
- `pitcher`, `batter`: IDs for players
- `release_speed`: Pitch velocity (mph)
- `pitch_type`: Type of pitch (FF, SL, CU, CH, etc.)
- `spin_rate_deprecated`: Spin rate of the pitch
- `launch_speed`: Exit velocity of batted ball (when contact made)
- `events`: Result of the pitch (strikeout, single, field_out, etc.)
- `game_year`: Year of the game (2017-2021)

## Notebook Structure

### 1. **Initialization** (Cell 2)
- Initializes PySpark with optimized configuration
- Sets driver/executor memory (8GB each)
- Enables Adaptive Query Execution (AQE)
- Configures RPC timeouts to prevent connection issues

### 2. **Data Loading** (Cell 3)
- Loads all 5 CSV files using `spark.read.csv()`
- Applies schema inference
- Combines via `reduce(union())` for efficient merging
- Output: 3,149,505 rows × 93 columns

### 3. **Filtering, Joins, Aggregations, & Transformations** (Cell 4)

#### Filters (2+):
- **Filter 1**: Remove null values for critical fields (`pitch_type`, `release_speed`, `pitcher`, `batter`)
- **Filter 2**: Valid pitch speeds (40-110 mph realistic range)

#### Column Transformations (withColumn):
- `pitch_speed_mph`: Copy of `release_speed`
- `is_fastball`: Binary flag (1 if fastball type, 0 otherwise)
- `is_breaking`: Binary flag for breaking pitches (curveballs, sliders, etc.)
- `is_offspeed`: Binary flag for changeups, screwballs
- `effective_speed`: Filled null values
- `game_year_int`: Cast year to integer

#### Complex Aggregations (groupBy + agg):
- **Pitcher Stats**: Group by pitcher, compute pitch count, avg speed, max speed, pitch type distribution
  - Filter: Only pitchers with 100+ pitches
  - Result: 1,307 pitchers
- **Batter Stats**: Group by batter, compute at-bat count, avg exit velocity, balls in play

#### Join Operation:
- Join pitcher stats back onto main dataset using pitcher ID
- Enables analysis of individual pitcher characteristics alongside pitch data

### 4. **SQL Queries & Optimization** (Cell 5)

#### Query 1: Top Pitchers by Average Release Speed
```sql
SELECT pitcher, pitch_count, avg_release_speed, ...
FROM pitcher_stats_view
WHERE pitch_count >= 200
ORDER BY avg_release_speed DESC
LIMIT 10
```
- **Optimization**: Filter pushed down (WHERE clause evaluated early)
- **Output**: Top 10 fastest pitchers

#### Query 2: Pitch Type Distribution by Year
```sql
SELECT game_year_int, pitch_type, COUNT(*), 
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY game_year_int), 2) as pct
FROM statcast_view
WHERE pitch_type IS NOT NULL AND release_speed > 50
GROUP BY game_year_int, pitch_type
ORDER BY game_year_int, count DESC
```
- **Optimization**: 
  - Early filter on pitch speed (release_speed > 50) reduces scan
  - Window function partitioned by year (localizes shuffle)
  - Percentage calculation avoids secondary shuffle
- **Output**: Pitch type usage trends over 5 years

#### Results Written to CSV
- Query 1 → `./output/top_pitchers/`
- Query 2 → `./output/pitch_distribution/`

### 5. **Performance Analysis** (Cell 6)

#### .explain() Output
Shows physical execution plans for:
- SQL Query 2 (pitch distribution with window function)
- Aggregation (pitcher stats groupBy)

#### Analysis Highlights

**Filter Pushdown (Early Filtering)**
- Filters on `pitch_type`, `release_speed` applied BEFORE groupBy
- Spark pushes predicates to the CSV scan level
- Reduces I/O and memory from 3.15M → 3.13M records immediately

**Column Pruning**
- Only selected columns appear in execution plans
- Unused columns dropped before shuffles
- Reduces shuffle I/O

**Partitioning Strategy**
- AQE enabled (`spark.sql.adaptive.enabled=true`) detects join/shuffle sizes
- Dynamically adjusts task counts based on intermediate result sizes
- Window functions partitioned by year to localize computation

**Avoiding Unnecessary Shuffles**
- Pitcher aggregation groups on single column (efficient)
- Small lookup tables broadcast joined (avoids shuffle)
- Window partition BY year keeps related rows co-located

**Bottlenecks Identified**
1. CSV parsing overhead (→ use Parquet in production)
2. Window function shuffle in Query 2 (expected; dataset is naturally year-grouped)
3. Large aggregation group cardinality (mitigated with filtering)

### 6. **Caching Optimization** (Cell 7)

#### Benchmark: .cache() Impact

**Setup**: 
- Create test DataFrame with filters (high-speed pitches, 2019+)
- Run aggregation (count by pitch type) with and without cache

**Results**:
```
Without cache:
  Run 1: 0.XXs, Run 2: 0.XXs → Average

With cache (MEMORY_AND_DISK):
  Materialize: 0.XXs
  Run 1: 0.XXs, Run 2: 0.XXs → Average

Improvement: XX% faster, XX.Xx speedup
```

**Insights**:
- Caching beneficial when repeating operations on same data
- Use `.persist(StorageLevel.MEMORY_AND_DISK)` for robustness (spills to disk if OOM)
- Call `.unpersist()` to free memory after use

### 7. **Actions vs Transformations** (Cell 8)

#### Demonstration: Lazy Execution

**Transformations (Lazy - No Execution)**:
```python
transform_1 = df.filter(col("release_speed") > 85)           # No compute
transform_2 = transform_1.filter(col("pitch_type").isin(...)) # No compute
transform_3 = transform_2.withColumn(...)                     # No compute
transform_4 = transform_3.select(...)                         # No compute
```

**Actions (Eager - Force Computation)**:
```python
count_result = transform_4.count()     # Triggers full execution
transform_4.show(3)                    # Triggers execution (shows results)
collected = transform_4.collect()      # Gathers results to driver
```

**Key Insight**: 
- Spark optimizes entire pipeline before .count()/.show()/.collect()
- Transforms are combined (filter pushdown, column pruning applied)
- Only one pass over data for multiple transforms

### 8. **Machine Learning: Pitch Classification** (Cell 9)

#### Task: Predict Pitch Type (Fastball vs Other)

**Data Preparation**:
- Target: `is_fastball` (binary: 1=fastball, 0=other pitch type)
- Features:
  - `release_speed`: Pitch velocity
  - `pitcher_avg_speed`: Pitcher's average speed
  - `spin_rate`: Pitch spin rate
  - `effective_speed`: Effective velocity

**Model**: Random Forest Classifier
- 10 trees, max depth 5 (fast training on sample)
- 80/20 train/test split
- Trained on 5% sample (~360k rows) for demo speed

**Performance**:
- ROC-AUC Score: ~0.9+ (high discrimination power)
- Feature Importances: Shows which features matter most

**Results**: 
- Most important features likely: `release_speed`, `effective_speed`
- Model can classify fastballs vs breaking/offspeed pitches with high accuracy

## Running the Notebook

### Prerequisites
1. **Java** must be installed and `JAVA_HOME` set
   ```bash
   java -version
   echo $JAVA_HOME
   ```

2. **PySpark** installed
   ```bash
   pip install pyspark>=3.4.0
   ```

3. **CSV data** in `data/` directory

### Steps
1. Open `statcast_analysis.ipynb` in Jupyter Lab / VS Code
2. Run cells top-to-bottom
3. Cell 2: SparkSession initialization (displays Spark UI URL)
4. Cell 3: Data loads (takes ~10-15s for all 5 years)
5. Cells 4-9: Analysis, queries, performance analysis, caching, ML
6. Monitor via Spark UI (URL printed in cell 2)

### Expected Runtime
- Data loading: ~15s
- Filtering/aggregations: ~20-30s
- SQL queries: ~15-20s
- .explain() (plan generation): <5s
- Caching benchmark: ~30-40s (running multiple times)
- ML training: ~10-15s (on 5% sample)
- **Total**: ~2-3 minutes end-to-end

### Output Files
- `./output/top_pitchers/part-*.csv`: Top 10 pitchers by avg speed
- `./output/pitch_distribution/part-*.csv`: Pitch type trends by year

## Performance Tips

### Memory Management
- Adjust `spark.driver.memory` and `spark.executor.memory` based on available RAM
- Notebook uses 8GB driver + 8GB executor; reduce if memory-constrained
- Use sampling (e.g., `df.sample(0.05)`) for exploratory work

### Spark Configuration
```python
.config("spark.sql.adaptive.enabled", "true")           # Dynamic query optimization
.config("spark.sql.shuffle.partitions", "200")          # Shuffle partition count
.config("spark.network.timeout", "600s")                # Extended timeout
.config("spark.executor.heartbeatInterval", "60s")      # Heartbeat frequency
```

### Writing Outputs
- Use `.coalesce(1)` before writing small CSVs (avoids file fragmentation)
- Use `.partitionBy(column)` for large outputs (speeds up future reads)
- Snappy compression: `.option("compression", "snappy")`

## Troubleshooting

### "Connection refused" / "JAVA_GATEWAY_EXITED"
- **Cause**: JVM crashed or Py4J can't connect
- **Fix**: 
  - Ensure `spark.driver.bindAddress = "127.0.0.1"`
  - Restart notebook kernel
  - Check Java: `java -version`

### Out of Memory (OOM)
- Reduce `spark.driver.memory` to available RAM
- Use sampling: `df.sample(0.05)`
- Check system resources: `free -h`

### Slow Performance
- Enable Adaptive Query Execution (already in config)
- Check Spark UI (http://localhost:4040) for stage details
- Use .explain() to identify bottlenecks

## References

- **Statcast Data**: https://baseballsavant.mlb.com/csv-downloads
- **PySpark Documentation**: https://spark.apache.org/docs/latest/api/python/
- **Spark SQL Tuning**: https://spark.apache.org/docs/latest/sql-performance-tuning.html
