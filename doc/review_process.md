# Process for a smooth code review

We put great emphasis on code reviews, every pull requery (PR) , big or small,
has to be approved by a reviewer with write access. Unfortunately, GitHub's
review tool has a lot of shortcomings, especially for longer review threads.
It can also make it hard to follow the changes that has been done to address
a review comment
([other people have complained about this too](https://github.com/github-community/community/discussions/9956)).

To address some of these shortcomings, we follow the following process. Before
sending a PR, please spend some time and fully understand this process to help
your reviewers:

- Please try to have _a single commit per each review round_. When you want
  to send your PR, if you have multiple commits,
  [squash them](https://stackoverflow.com/questions/5189560/how-to-squash-my-last-x-commits-together).
  Also, when you are responding to review comments, make sure to create exactly
  _one commit to address all required changes_. You can use
  [--amend](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---amend)
  feature of git commit. This is to help your reviewer see all of the changes
  since last review round in a single commit.

- Please _never_ rewrite (e.g., `--amend`) an old commit that has been reviewed.
  This will lose GitHub comment threads attached to that commit.
  Note this also means never use
  [git rebase](https://git-scm.com/docs/git-rebase) on a branch that is under
  review, because that basically rewrites the old commits.
  To merge changes to the upstream repo that happen during the review,
  you can go to your `master` branch, pull the changes from the upstream
  repo, and then `merge` your `master` branch into your feature branch from
  which you sent the PR.

- When sending a PR, _disable_ ["Allow edits by maintainers”"](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork);
  and if you are a reviewer, never push a change (even a master merge) to a
  review branch. This will make it harder to maintain the local dev branch.

- When responding to unresolved comments, please make sure you respond to all of
  them. Even if you simply do what the reviewer has suggested, add a "Done"
  comment before resolving the thread. This acts as a reminder to the reviewer
  about all of the requested changes from the last review round.

- Finally, please make sure that you fill the `TESTED:` part of your PR
  description, especially for larger PRs. This will help your reviewer better
  understand what your change is supposed to do and how you have made sure it
  does that.

Let's look at a [sample PR](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/pull/152).
This PR is sent for review on 3 Apr 2021 and it included two commits; it
would have been better if the author had squashed those. After the first round
of review comments was received, on 12 Apr 2021
[this commit](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/pull/152/commits/ff6b787190454ff2f61900d0f2f8d5a432e4d147)
was created to address _all_ of those comments. Note that it is very easy for
reviewers to see every changes that have been done to address review comments.
At the same time
[this merge commit](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/pull/152/commits/a60f82cc72569d517b71b2f40092c3655acef5b0)
was also added to update the PR branch with other changes that happened to the
upstream repo while this PR was being reviewed (note `git merge` was used
not `git rebase`). Finally, take a look at the `TESTED:` part of the PR
description.