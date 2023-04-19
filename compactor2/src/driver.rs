use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams, PartitionId};
use futures::StreamExt;
use observability_deps::tracing::info;
use parquet_file::ParquetFilePath;
use tokio::sync::watch::Sender;
use tracker::InstrumentedAsyncSemaphore;

use crate::{
    components::{
        changed_files_filter::SavedParquetFileState,
        scratchpad::Scratchpad,
        timeout::{timeout_with_progress_checking, TimeoutWithProgress},
        Components,
    },
    error::{DynError, ErrorKind, SimpleError},
    file_classification::{FileClassification, FilesForProgress, FilesToSplitOrCompact},
    partition_info::PartitionInfo,
    PlanIR,
};

/// Tries to compact all eligible partitions, up to
/// partition_concurrency at a time.
pub async fn compact(
    partition_concurrency: NonZeroUsize,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: &Arc<Components>,
) {
    components
        .partition_stream
        .stream()
        .map(|partition_id| {
            let components = Arc::clone(components);

            compact_partition(
                partition_id,
                partition_timeout,
                Arc::clone(&job_semaphore),
                components,
            )
        })
        .buffer_unordered(partition_concurrency.get())
        .collect::<()>()
        .await;
}

async fn compact_partition(
    partition_id: PartitionId,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
) {
    info!(partition_id = partition_id.get(), "compact partition",);
    let mut scratchpad = components.scratchpad_gen.pad();

    let res = timeout_with_progress_checking(partition_timeout, |transmit_progress_signal| {
        let components = Arc::clone(&components);
        async {
            try_compact_partition(
                partition_id,
                job_semaphore,
                components,
                scratchpad.as_mut(),
                transmit_progress_signal,
            )
            .await
        }
    })
    .await;

    let res = match res {
        // If `try_compact_partition` timed out and didn't make any progress, something is wrong
        // with this partition and it should get added to the `skipped_compactions` table by
        // sending a timeout error to the `partition_done_sink`.
        TimeoutWithProgress::NoWorkTimeOutError => Err(Box::new(SimpleError::new(
            ErrorKind::Timeout,
            "timeout without making any progress",
        )) as _),
        // If `try_compact_partition` timed out but *did* make some progress, this is fine, don't
        // add it to the `skipped_compactions` table.
        TimeoutWithProgress::SomeWorkTryAgain => Ok(()),
        // If `try_compact_partition` finished before the timeout, return the `Result` that it
        // returned. If an error was returned, there could be something wrong with the partiton;
        // let the `partition_done_sink` decide if the error means the partition should be added
        // to the `skipped_compactions` table or not.
        TimeoutWithProgress::Completed(res) => res,
    };
    components
        .partition_done_sink
        .record(partition_id, res)
        .await;

    scratchpad.clean().await;
    info!(partition_id = partition_id.get(), "compacted partition",);
}

/// Main function to compact files of a single partition.
///
/// Input: any files in the partitions (L0s, L1s, L2s)
/// Output:
/// 1. No overlapped  L0 files
/// 2. Up to N non-overlapped L1 and L2 files,  subject to  the total size of the files.
///
/// N: config max_number_of_files
///
/// Note that since individual files also have a maximum size limit, the
/// actual number of files can be more than  N.  Also since the Parquet format
/// features high and variable compression (page splits, RLE, zstd compression),
/// splits are based on estimated output file sizes which may deviate from actual file sizes
///
/// Algorithms
///
/// GENERAL IDEA OF THE CODE: DIVIDE & CONQUER  (we have not used all of its power yet)
///
/// The files are split into non-time-overlaped branches, each is compacted in parallel.
/// The output of each branch is then combined and re-branch in next round until
/// they should not be compacted based on defined stop conditions.
///
/// Example: Partition has 7 files: f1, f2, f3, f4, f5, f6, f7
///  Input: shown by their time range
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
///
/// - Round 1: Assuming 7 files are split into 2 branches:
///  . Branch 1: has 3 files: f1, f2, f3
///  . Branch 2: has 4 files: f4, f5, f6, f7
///          |<------- Branch 1 -------------->|  |<-- Branch 2 -->|
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
///
///    Output: 3 files f8, f9, f10
///          |<------- Branch 1 -------------->|  |<-- Branch 2--->|
///          |---------f8---------------|--f9--|  |-----f10--------|
///
/// - Round 2: 3 files f8, f9, f10 are in one branch and compacted into 2 files
///    Output: two files f11, f12
///          |-------------------f11---------------------|---f12---|
///
/// - Stop condition meets and the final output is f11 & F12
///
/// The high level flow is:
///
///   . Mutiple rounds, each round process mutltiple branches. Each branch includes at most 200 files
///   . Each branch will compact files lowest level (aka start-level) into its next level (aka target-level), either:
///      - Compact many L0s into fewer and larger L0s. Start-level = target-level = 0
///      - Compact many L1s into fewer and larger L1s. Start-level = target-level = 1
///      - Compact (L0s & L1s) to L1s if there are L0s. Start-level = 0, target-level = 1
///      - Compact (L1s & L2s) to L2s if no L0s. Start-level = 1, target-level = 2
///      - Split L0s each of which overlaps with more than 1 L1s into many L0s, each overlaps with at most one L1 files
///      - Split L1s each of which overlaps with more than 1 L2s into many L1s, each overlaps with at most one L2 files
///   . Each branch does find non-overlaps and upgragde files to avoid unecessary recompacting.
///     The actually split files:
///      1. files_to_keep: do not compact these files because they are already higher than target level
///      2. files_to_upgrade: upgrade this initial-level files to target level because they are not overlap with
///          any target-level and initial-level files and large enough (> desired max size)
///      3. files_to_split_or_compact: this is either files to split or files to compact and will be handled accordingly

///
/// Example: 4 files: two L0s, two L1s and one L2
///  Input:
///                                      |-L0.1-||------L0.2-------|
///                  |-----L1.1-----||--L1.2--|
///     |----L2.1-----|
///
///  - Round 1: There are L0s, let compact L0s with L1s. But let split them first:
///    . files_higher_keep: L2.1 (higher leelve than targetlevel) and L1.1 (not overlapped wot any L0s)
///    . files_upgrade: L0.2
///    . files_compact: L0.1, L1.2
///    Output: 4 files
///                                               |------L1.4-------|
///                  |-----L1.1-----||-new L1.3 -|        ^
///     |----L2.1-----|                  ^               |
///                                      |        result of upgrading L0.2
///                            result of compacting L0.1, L1.2
///
///  - Round 2: Compact those 4 files
///    Output: two L2 files
///     |-----------------L2.2---------------------||-----L2.3------|
///
/// Note:
///   . If there are no L0s files in the partition, the first round can just compact L1s and L2s to L2s
///   . Round 2 happens or not depends on the stop condition
async fn try_compact_partition(
    partition_id: PartitionId,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
    scratchpad_ctx: &mut dyn Scratchpad,
    transmit_progress_signal: Sender<bool>,
) -> Result<(), DynError> {
    let mut files = components.partition_files_source.fetch(partition_id).await;
    let partition_info = components.partition_info_source.fetch(partition_id).await?;

    // loop for each "Round", consider each file in the partition
    loop {
        let round_info = components
            .round_info_source
            .calculate(&partition_info, &files)
            .await?;

        // This is the stop condition which will be different for different version of compaction
        // and describe where the filter is created at version_specific_partition_filters function
        if !components
            .partition_filter
            .apply(&partition_info, &files)
            .await?
        {
            return Ok(());
        }

        let (files_now, files_later) = components.round_split.split(files, round_info);

        // Each branch must not overlap with each other
        let branches = components
            .divide_initial
            .divide(files_now, round_info)
            .into_iter();

        let mut files_next = files_later;
        // loop for each "Branch"
        for branch in branches {
            // Keep the current state as a check to make sure this is the only compactor modifying this branch's
            // files. Check that the catalog state for the files in this set is the same before committing and, if not,
            // throw away the compaction work we've done.
            let saved_parquet_file_state = SavedParquetFileState::from(&branch);

            let input_paths: Vec<ParquetFilePath> =
                branch.iter().map(ParquetFilePath::from).collect();

            // Identify the target level and files that should be
            // compacted together, upgraded, and kept for next round of
            // compaction
            let FileClassification {
                target_level,
                files_to_make_progress_on,
                files_to_keep,
            } = components
                .file_classifier
                .classify(&partition_info, &round_info, branch);

            // Evaluate whether there's work to do or not based on the files classified for
            // making progress on. If there's no work to do, return early.
            //
            // Currently, no work to do mostly means we are unable to compact this partition due to
            // some limitation such as a large file with single timestamp that we cannot split in
            // order to further compact.
            if !components
                .post_classification_partition_filter
                .apply(&partition_info, &files_to_make_progress_on)
                .await?
            {
                return Ok(());
            }

            let FilesForProgress {
                upgrade,
                split_or_compact,
            } = files_to_make_progress_on;

            // Compact
            let created_file_params = run_plans(
                split_or_compact.clone(),
                &partition_info,
                &components,
                target_level,
                Arc::clone(&job_semaphore),
                scratchpad_ctx,
            )
            .await?;

            // upload files to real object store
            let created_file_params =
                upload_files_to_object_store(created_file_params, scratchpad_ctx).await;

            // clean scratchpad
            scratchpad_ctx.clean_from_scratchpad(&input_paths).await;

            // Update the catalog to reflect the newly created files, soft delete the compacted
            // files and update the upgraded files
            let files_to_delete = split_or_compact.into_files();
            let (created_files, upgraded_files) = update_catalog(
                Arc::clone(&components),
                partition_id,
                saved_parquet_file_state,
                files_to_delete,
                upgrade,
                created_file_params,
                target_level,
            )
            .await;

            // Extend created files, upgraded files and files_to_keep to files_next
            files_next.extend(created_files);
            files_next.extend(upgraded_files);
            files_next.extend(files_to_keep);

            // Report to `timeout_with_progress_checking` that some progress has been made; stop
            // if sending this signal fails because something has gone terribly wrong for the other
            // end of the channel to not be listening anymore.
            if let Err(e) = transmit_progress_signal.send(true) {
                return Err(Box::new(e));
            }
        }

        files = files_next;
    }
}

/// Compact or split given files
async fn run_plans(
    split_or_compact: FilesToSplitOrCompact,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<Vec<ParquetFileParams>, DynError> {
    // stage files
    let input_uuids_inpad = scratchpad_ctx
        .load_to_scratchpad(&split_or_compact.file_input_paths())
        .await;

    let plans = components.ir_planner.create_plans(
        Arc::clone(partition_info),
        target_level,
        split_or_compact,
        input_uuids_inpad,
    );
    let capacity = plans.iter().map(|p| p.n_output_files()).sum();
    let mut created_file_params = Vec::with_capacity(capacity);

    for plan_ir in plans
        .into_iter()
        .filter(|plan| !matches!(plan, PlanIR::None { .. }))
    {
        created_file_params.extend(
            execute_plan(
                plan_ir,
                partition_info,
                components,
                Arc::clone(&job_semaphore),
            )
            .await?,
        )
    }
    .buffer_unordered(4)
    .collect::<()>();

    Ok(created_file_params)
}

async fn execute_plan(
    plan_ir: PlanIR,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
) -> Result<Vec<ParquetFileParams>, DynError> {
    let create = {
        // Adjust concurrency based on the column count in the partition.
        let permits = compute_permits(job_semaphore.total_permits(), partition_info.column_count());

        info!(
            partition_id = partition_info.partition_id.get(),
            jobs_running = job_semaphore.holders_acquired(),
            jobs_pending = job_semaphore.holders_pending(),
            permits_needed = permits,
            permits_acquired = job_semaphore.permits_acquired(),
            permits_pending = job_semaphore.permits_pending(),
            "requesting job semaphore",
        );

        // draw semaphore BEFORE creating the DataFusion plan and drop it directly AFTER finishing the
        // DataFusion computation (but BEFORE doing any additional external IO).
        //
        // We guard the DataFusion planning (that doesn't perform any IO) via the semaphore as well in case
        // DataFusion ever starts to pre-allocate buffers during the physical planning. To the best of our
        // knowledge, this is currently (2023-01-25) not the case but if this ever changes, then we are prepared.
        let permit = job_semaphore
            .acquire_many(permits, None)
            .await
            .expect("semaphore not closed");
        info!(
            partition_id = partition_info.partition_id.get(),
            permits, "job semaphore acquired",
        );

        let plan = components
            .df_planner
            .plan(&plan_ir, Arc::clone(partition_info))
            .await?;
        let streams = components.df_plan_exec.exec(plan);
        let job = components.parquet_files_sink.stream_into_file_sink(
            streams,
            Arc::clone(partition_info),
            plan_ir.target_level(),
            &plan_ir,
        );

        // TODO: react to OOM and try to divide branch
        let res = job.await;

        drop(permit);
        info!(
            partition_id = partition_info.partition_id.get(),
            "job semaphore released",
        );

        res?
    };

    Ok(create)
}

async fn upload_files_to_object_store(
    created_file_params: Vec<ParquetFileParams>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Vec<ParquetFileParams> {
    // Ipload files to real object store
    let output_files: Vec<ParquetFilePath> = created_file_params.iter().map(|p| p.into()).collect();
    let output_uuids = scratchpad_ctx.make_public(&output_files).await;

    // Update file params with object_store_id
    created_file_params
        .into_iter()
        .zip(output_uuids)
        .map(|(f, uuid)| ParquetFileParams {
            object_store_id: uuid,
            ..f
        })
        .collect()
}

async fn fetch_and_save_parquet_file_state(
    components: &Components,
    partition_id: PartitionId,
) -> SavedParquetFileState {
    let catalog_files = components.partition_files_source.fetch(partition_id).await;
    SavedParquetFileState::from(&catalog_files)
}

/// Update the catalog to create, soft delete and upgrade corresponding given input
/// to provided target level
/// Return created and upgraded files
async fn update_catalog(
    components: Arc<Components>,
    partition_id: PartitionId,
    saved_parquet_file_state: SavedParquetFileState,
    files_to_delete: Vec<ParquetFile>,
    files_to_upgrade: Vec<ParquetFile>,
    file_params_to_create: Vec<ParquetFileParams>,
    target_level: CompactionLevel,
) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
    let current_parquet_file_state =
        fetch_and_save_parquet_file_state(&components, partition_id).await;

    // Right now this only logs; in the future we might decide not to commit these changes
    let _ignore = components
        .changed_files_filter
        .apply(&saved_parquet_file_state, &current_parquet_file_state);

    let created_ids = components
        .commit
        .commit(
            partition_id,
            &files_to_delete,
            &files_to_upgrade,
            &file_params_to_create,
            target_level,
        )
        .await;

    // Update created ids to their corresponding file params
    let created_file_params = file_params_to_create
        .into_iter()
        .zip(created_ids)
        .map(|(params, id)| ParquetFile::from_params(params, id))
        .collect::<Vec<_>>();

    // Update compaction_level for the files_to_upgrade
    let upgraded_files = files_to_upgrade
        .into_iter()
        .map(|mut f| {
            f.compaction_level = target_level;
            f
        })
        .collect::<Vec<_>>();

    (created_file_params, upgraded_files)
}

// SINGLE_THREADED_COLUMN_COUNT is the number of columns requiring a partition be compacted single threaded.
const SINGLE_THREADED_COLUMN_COUNT: usize = 100;

// Determine how many permits must be acquired from the concurrency limiter semaphore
// based on the column count of this job and the total permits (concurrency).
fn compute_permits(
    total_permits: usize, // total number of permits (max concurrency)
    columns: usize,       // column count for this job
) -> u32 {
    if columns >= SINGLE_THREADED_COLUMN_COUNT {
        // this job requires all permits, forcing it to run by itself.
        return total_permits as u32;
    }

    // compute the share (linearly scaled) of total permits this job requires
    let share = columns as f64 / SINGLE_THREADED_COLUMN_COUNT as f64;

    // Square the share so the required permits is non-linearly scaled.
    // See test cases below for detail, but this makes it extra permissive of low column counts,
    // but still gets to single threaded by SINGLE_THREADED_COLUMN_COUNT.
    let permits = total_permits as f64 * share * share;

    if permits < 1.0 {
        return 1;
    }

    permits as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_limits() {
        assert_eq!(compute_permits(100, 1), 1); // 1 column still takes 1 permit
        assert_eq!(compute_permits(100, SINGLE_THREADED_COLUMN_COUNT / 10), 1); // 10% of the max column count takes 1% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 2 / 10),
            4
        ); // 20% of the max column count takes 4% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 3 / 10),
            9
        ); // 30% of the max column count takes 9% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 4 / 10),
            16
        ); // 40% of the max column count takes 16% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 5 / 10),
            25
        ); // 50% of the max column count takes 25% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 6 / 10),
            36
        ); // 60% of the max column count takes 36% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 7 / 10),
            49
        ); // 70% of the max column count takes 49% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 8 / 10),
            64
        ); // 80% of the max column count takes 64% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 9 / 10),
            81
        ); // 90% of the max column count takes 81% of total permits
        assert_eq!(compute_permits(100, SINGLE_THREADED_COLUMN_COUNT), 100); // 100% of the max column count takes 100% of total permits
        assert_eq!(compute_permits(100, 10000), 100); // huge column count takes exactly all permits (not more than the total)
    }
}
