# Contributing to TreodeDB

## Sign the CLA

If you wish to contribute to TreodeDB, you must first sign the [Contributor License Agreement (CLA)][cla-individual]. If your employer has rights your intellectual property, your employer will need to sign the [Corporate CLA][cla-corporate]. 

The Treode team cannot accept contributions from someone who has not submitted the appropriate paperwork first.


## Asking Questions

1. Search first as your question might already be answered. If so, you'll get your answer faster by searching than by posting.

    a. Search the [online fourm][online-forum]. You do not need an account to search and read the forum.
    
    b. [Search Stack Overflow][stackoverflow-read] using the tag `treode`.
    
    c. Search using your favorite search engine.

2. If you haven't found an answer, post the question.

    a. Ask in the [online fourm][online-forum]. You can use your GitHub account to login; click the "Log In" button. Or you create an account specifically for the forum if that's your preference.

    b. [Ask on Stack Overflow][stackoverflow-ask] using the tag `treode`. Remember to keep your question oriented towards [specific programming problems that have an answer][stackoverflow-howto].



## Reporting Bugs

1. Update to the latest release; the bug may be resolved.

2. Search the [online forum][online-forum]; there may already be hints or workarounds.

3. Report the bug. We are flexible about where you report it.

    a. Raise the issue in the [online fourm][online-forum]. This is the most informal and flexible method. You can expedite the discussion if you state clearly what you did, what actually happened, and what you had expected to happen.

    c. Submit a [pull request](#pull-request) with a failing test case; you do not need to create an issue first.
    
    d. Submit a [pull request](#pull-request) with a test case and fix; you do not need to create an issue first.



## Requesting Features

Request new features through the [online fourm][online-forum]. Feature requests posted to the issue database will be politely closed. Search there first as there may already be a discussion, development or other activity related to your request.

The online forum provides everyone a chance to discuss the a request until we clearly understand the functionality, its relevance to general users, and possible implementations.


## Building, Testing and Debugging

Checkout the [Contributor category][online-forum-contributor] of the [online forum][online-forum].


## Submitting a Pull Request
<a name="pull-request"></a>

You can submit changes the GitHub way: by [submitting a pull request][using-pull-requests]. We mostly follow the GitHub code review process.

We use the GitHub code review tool until it comes time to merge. Then, we do not push the big green "Merge pull request" button; see ["Merge Pull Request" Considered Harmful][merge-harmful]. Instead, we cleanup the pull request, and move it to a `merge/` branch. The [CI machine][build-status] tests the change, produces build artifacts, and merges it into the master branch.

Also, we do not nit-pick in our reviews. We provide material feedback. When your change reaches some reasonable state, we take ownership of it, and we fix our nits at that time.



[build-status]: https://build.treode.com

[cla-individual]: https://treode.github.io/store/cla-individual.html

[cla-corporate]: https://treode.github.io/store/cla-corporate.html

[online-forum]: https://forum.treode.com "Online Forum for Users and Developers of Treode"

[online-forum-contributor]: https://forum.treode.com/category/contributor "Contributor Topics in the Online Forum"

[merge-harmful]: http://blog.spreedly.com/2014/06/24/merge-pull-request-considered-harmful "&rquo;Merge pull request&lquo; Considered Harmful"

[new-issue]: https://github.com/Treode/store/issues/new "Create a New Issue"

[stackoverflow]: http://stackoverflow.com "Stack Overflow"

[stackoverflow-ask]: http://stackoverflow.com/questions/ask?tags=treode "Post a question on Stack Overflow tagged with treode"

[stackoverflow-howto]: http://stackoverflow.com/help/how-to-ask "How do I ask a good question?"

[stackoverflow-read]: http://stackoverflow.com/questions/tagged/treode "Read questions on Stack Overflow tagged with treode"

[using-pull-requests]: https://help.github.com/articles/using-pull-requests "Using Pull Requests"
