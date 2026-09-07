select
    comment_id,
    ticket_id,
    author_role,
    body,
    nullif(is_public, '')::boolean       as is_public,
    nullif(created_at, '')::timestamptz  as created_at
from {{ source('bronze', 'zendesk__ticket_comments') }}
