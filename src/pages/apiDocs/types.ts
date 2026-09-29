export type ApiMethod = "GET" | "POST";

export type ApiCategory =
  | "overview"
  | "species"
  | "disease"
  | "network"
  | "analysis"
  | "similarity"
  | "studies"
  | "download";

export interface ApiParam {
  name: string;
  type: string;
  required: boolean;
  description: string;
}

export interface ApiEndpoint {
  method: ApiMethod;
  path: string;
  category: ApiCategory;
  summary: string;
  description: string;
  params?: ApiParam[];
  defaultQuery?: Record<string, string | number | boolean>;
  defaultBody?: Record<string, unknown>;
  responseSchema: string[];
  errorCodes: number[];
}
