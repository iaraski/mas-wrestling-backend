create table if not exists public.otp_codes (
  email text primary key,
  code_hash text not null,
  expires_at timestamptz not null,
  attempts int not null default 0,
  last_sent_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists otp_codes_expires_at_idx on public.otp_codes (expires_at);

create or replace function public.set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists otp_codes_set_updated_at on public.otp_codes;
create trigger otp_codes_set_updated_at
before update on public.otp_codes
for each row execute function public.set_updated_at();

alter table public.otp_codes enable row level security;

drop policy if exists "service role only" on public.otp_codes;
create policy "service role only" on public.otp_codes
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
