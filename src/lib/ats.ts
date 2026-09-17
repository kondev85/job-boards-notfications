export const ATS_OPTIONS = [
  { value: "ashby", label: "Ashby" },
  { value: "greenhouse", label: "Greenhouse" },
  { value: "lever", label: "Lever" },
  { value: "smartrecruiters", label: "SmartRecruiters" },
  { value: "workday", label: "Workday" },
  { value: "recruitee", label: "Recruitee" },
  { value: "teamtailor", label: "Teamtailor" },
  { value: "workable", label: "Workable" },
] as const;

const ATS_LABELS = new Map<string, string>(
  ATS_OPTIONS.map((option) => [option.value, option.label]),
);

export function atsLabel(value: string): string {
  return ATS_LABELS.get(value) ?? value;
}