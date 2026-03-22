export type Result<T> = {
  ok: true;
  data: T;
};

export type Failure = {
  ok: false;
  error: string;
};
