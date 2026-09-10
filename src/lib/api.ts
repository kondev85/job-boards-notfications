export type JobStatus = "new" | "saved" | "applied" | "rejected";
export interface Me { user_id:number; name:string; email:string; target_roles:string[]; target_industries:string[]; min_match_score:number|null }
export interface MatchedJob { job_id:number; ats:string; external_id:string; company:string; title:string; location_raw:string; workplace_type:string; published_at:string; job_url:string; score:number; status:JobStatus }
export interface SearchRun { run_id:number; progress:number; status:"queued"|"running"|"completed"|"completed_with_warnings"|"failed"; cutoff:string; run_type:"manual"|"scheduled"; error?:string }
export interface Profile {
  [key:string]: any;
  name:string; profile_text:string|null; cv_text:string|null;
  target_roles:string[]|null; target_industries:string[]|null;
  base_city:string|null; base_country:string|null;
  remote_allowed:boolean|null; onsite_allowed:boolean|null;
  onsite_max_distance_km:number|null; hybrid_allowed:boolean|null;
  hybrid_max_distance_km:number|null; willing_to_relocate:boolean|null;
  relocation_cities:string[]|null; relocation_countries:string[]|null;
  min_match_score:number|null;
  profile_generated_at?:string|null; profile_model?:string|null; profile_version?:string|null;
}
export interface Recommendation {
  review_id?: number; job_id?: number;
  title?: string|null; company?: string|null; job_url?: string|null; ats?: string|null;
  external_id?: string|null; location_raw?: string|null; workplace_type?: string|null;
  published_at?: string|null; description_text?: string|null;
  final_fit_score?: number|null; final_recommendation?: string|null;
  final_strengths?: string[]|null; final_concerns?: string[]|null; final_rationale?: string|null;
  fit_score?: number|null; recommendation?: string|null; strengths?: string[]|null;
  concerns?: string[]|null; rationale?: string|null;
  batch_fit_score?: number|null; batch_recommendation?: string|null;
  batch_strengths?: string[]|null; batch_concerns?: string[]|null; batch_rationale?: string|null;
  [key:string]: unknown;
}
async function request<T>(url:string, init?:RequestInit):Promise<T> { const r=await fetch(url, { credentials:"same-origin", ...init }); if (!r.ok) throw new Error((await r.json().catch(()=>({}))).error || r.statusText); return r.json(); }
export const getMe = () => request<Me>("/api/me");
export const getProfileSummary = () => request<Record<string,unknown>>("/api/profile/summary");
export const updateProfile = (profile:Partial<Profile>) => request<Profile>("/api/profile",{method:"PATCH",headers:{"Content-Type":"application/json"},body:JSON.stringify(profile)});
export const getBoardOptions = () => request<unknown[]>("/api/boards/options");
export const getMatchedJobs = (query:Record<string,string|number>={}) => request<{rows:MatchedJob[];total:number;page:number;limit:number}>(`/api/jobs/matched?${new URLSearchParams(Object.entries(query).map(([k,v])=>[k,String(v)]))}`);
export const getLatestRecommendations = () => request<Recommendation[]>("/api/recommendations/latest");
export const updateJobStatus = (jobId:number,status:JobStatus) => request<{status:JobStatus}>(`/api/jobs/${jobId}/status`, { method:"PATCH", headers:{"Content-Type":"application/json"}, body:JSON.stringify({status}) });
export const launchSearch = (body:{cutoff:string;scope:"all"|"ats"|"boards";ats?:string;boardIds?:number[]}) => request<{runId:number}>("/api/search-runs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
export const getSearchRun = (id:number) => request<SearchRun>(`/api/search-runs/${id}`);
export const getRecentSearchRuns = () => request<SearchRun[]>("/api/search-runs");
export const importCsv = (file:File) => { const body=new FormData(); body.append("file",file); return request<{imported:number;rows:number}>("/api/import",{method:"POST",body}); };
export const exportCsvUrl = "/api/export.csv";