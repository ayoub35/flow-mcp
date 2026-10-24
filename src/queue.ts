import { randomUUID } from "node:crypto";

export type JobStatus = "queued" | "running" | "done" | "failed" | "cancelled";

export interface SceneParams {
  prompt: string;
  type: "video" | "image";
  max_credits: number;
  duration?: 4 | 6 | 8 | 10;
  resolution?: "360p" | "720p";
  model?: string;
  aspect_ratio?: "16:9" | "9:16" | "1:1" | "4:3" | "3:4";
  variants?: number;
  first_frame?: string;
  last_frame?: string;
  reference_images?: string[];
  chain_from?: string;
  reference_from?: string;
  edit_asset?: string;
  agent_scenes?: string[];
  // The scene number each agent scene is saved as. A retry of scenes 7 and 12 keeps scene-07 and scene-12; without it
  // the numbers run on from file_stem.
  agent_numbers?: number[];
  // Reference images for an agent batch: attached once as ingredients in the agent composer (absolute paths, uploaded
  // once, or "asset:<title>"), with a short name each, plus which of them every scene uses (names, per scene).
  agent_refs?: { image: string; name: string }[];
  agent_scene_refs?: string[][];
  // Flow asset names of agent_refs after upload, so a retry round re-attaches without uploading again.
  agent_ref_assets?: string[];
  continue_from?: string;
  attach?: string[];
  continue_mode?: "exact" | "likeness";
  retries?: number;
  download_quality?: "original" | "upscaled";
  output_dir: string;
  file_stem: string;
}

export interface Job {
  id: string;
  status: JobStatus;
  params: SceneParams;
  files: string[];
  attempts: number;
  credits?: number;
  progress?: string;
  note?: string;
  error?: string;
  // Scene numbers a batch still could not make after all its own retries; flow_retry redoes just these.
  failed_scenes?: number[];
  createdAt: string;
  finishedAt?: string;
}

export type JobRunner = (job: Job) => Promise<string[]>;

// Flow is driven like a careful person would: one generation at a time, with a
// randomised pause between them. Images cost nothing and finish fast, so they get a shorter gap; videos spend
// credits and keep the long one. Never overlap generations to go faster: starting image prompts 2-5 s apart while
// earlier ones still rendered made Flow fail them with "We noticed some unusual activity" (2026-10-24). For parallel
// images use flow_agent_images, which is Flow's own feature.
const PAUSE_MIN_MS = Number(process.env.FLOW_MCP_PAUSE_MIN_S ?? 25) * 1000;
const PAUSE_MAX_MS = Number(process.env.FLOW_MCP_PAUSE_MAX_S ?? 70) * 1000;
const IMAGE_PAUSE_MIN_MS = Number(process.env.FLOW_MCP_IMAGE_PAUSE_MIN_S ?? 10) * 1000;
const IMAGE_PAUSE_MAX_MS = Number(process.env.FLOW_MCP_IMAGE_PAUSE_MAX_S ?? 20) * 1000;

export class JobQueue {
  private jobs = new Map<string, Job>();
  private working = false;
  private lastFinished = 0;
  pausedReason: string | null = null;

  constructor(private runner: JobRunner) {}

  add(params: SceneParams): Job {
    const job: Job = {
      id: randomUUID().slice(0, 8),
      status: "queued",
      params,
      files: [],
      attempts: 0,
      createdAt: new Date().toISOString(),
    };
    this.jobs.set(job.id, job);
    void this.work();
    return job;
  }

  get busy(): boolean {
    return this.list().some((j) => j.status === "running");
  }

  get(id: string): Job | undefined {
    return this.jobs.get(id);
  }

  list(): Job[] {
    return [...this.jobs.values()];
  }

  // Puts a failed or cancelled job back in line with the same settings and file name.
  retry(id: string): Job | undefined {
    const job = this.jobs.get(id);
    if (job && (job.status === "failed" || job.status === "cancelled")) {
      job.status = "queued";
      job.error = undefined;
      job.finishedAt = undefined;
      void this.work();
    }
    return job;
  }

  cancel(id: string): Job | undefined {
    const job = this.jobs.get(id);
    if (job && job.status === "queued") {
      job.status = "cancelled";
      job.finishedAt = new Date().toISOString();
    }
    return job;
  }

  private async work(): Promise<void> {
    if (this.working) return;
    this.working = true;
    try {
      for (;;) {
        const job = this.list().find((j) => j.status === "queued");
        if (!job) return;
        if (this.lastFinished) {
          const [min, max] =
            job.params.type === "image" ? [IMAGE_PAUSE_MIN_MS, IMAGE_PAUSE_MAX_MS] : [PAUSE_MIN_MS, PAUSE_MAX_MS];
          const pause = min + Math.random() * (max - min);
          const remaining = this.lastFinished + pause - Date.now();
          if (remaining > 0) {
            this.pausedReason = `pacing: next generation in ${Math.ceil(remaining / 1000)}s`;
            await new Promise((r) => setTimeout(r, remaining));
            this.pausedReason = null;
          }
        }
        if (job.status !== "queued") continue;
        job.status = "running";
        job.attempts++;
        try {
          job.files = await this.runner(job);
          job.status = "done";
          job.error = undefined;
        } catch (err) {
          job.error = err instanceof Error ? err.message : String(err);
          // Failed tiles are already retried inside Flow (its own Retry button); only an agent that produced nothing
          // is re-queued here. Timeouts are never retried automatically: they may still have spent credits.
          const retryable = /agent finished without creating/i.test(job.error);
          job.status = retryable && job.attempts <= (job.params.retries ?? 1) ? "queued" : "failed";
          // Once Google's abuse check has refused one generation, every queued job would only knock again.
          if (/unusual activity/i.test(job.error)) {
            for (const other of this.list().filter((j) => j.status === "queued")) {
              other.status = "cancelled";
              other.error = "Cancelled: Flow flagged unusual activity on an earlier job in the queue.";
              other.finishedAt = new Date().toISOString();
            }
          }
        }
        job.finishedAt = new Date().toISOString();
        this.lastFinished = Date.now();
      }
    } finally {
      this.working = false;
    }
  }
}
