from httmock import HTTMock, response
from urlparse import parse_qsl
import unittest, os

os.environ['GITHUB_TOKEN'] = ''
from app import app

class TestHook (unittest.TestCase):

    def setUp(self):
        '''
        '''
        self.client = app.test_client()

    def response_content(self, url, request):
        '''
        '''
        query = dict(parse_qsl(url.query))
        
        if (url.hostname, url.path) == ('api.github.com', '/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-alameda_county.json') and query.get('ref', '').startswith('e91fbc'):
            data = '''{
              "name": "us-ca-alameda_county.json",
              "path": "sources/us-ca-alameda_county.json",
              "sha": "c9cd0ed30256ae64d5924b03b0423346501b92d8",
              "size": 745,
              "url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-alameda_county.json?ref=e91fbc420f08890960f50f863626e1062f922522",
              "html_url": "https://github.com/openaddresses/hooked-on-sources/blob/e91fbc420f08890960f50f863626e1062f922522/sources/us-ca-alameda_county.json",
              "git_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs/c9cd0ed30256ae64d5924b03b0423346501b92d8",
              "download_url": "https://raw.githubusercontent.com/openaddresses/hooked-on-sources/e91fbc420f08890960f50f863626e1062f922522/sources/us-ca-alameda_county.json",
              "type": "file",
              "content": "ewogICAgImNvdmVyYWdlIjogewogICAgICAgICJVUyBDZW5zdXMiOiB7CiAg\\nICAgICAgICAgICJnZW9pZCI6ICIwNjAwMSIsCiAgICAgICAgICAgICJuYW1l\\nIjogIkFsYW1lZGEgQ291bnR5IiwKICAgICAgICAgICAgInN0YXRlIjogIkNh\\nbGlmb3JuaWEiCiAgICAgICAgfSwKICAgICAgICAiY291bnRyeSI6ICJ1cyIs\\nCiAgICAgICAgInN0YXRlIjogImNhIiwKICAgICAgICAiY291bnR5IjogIkFs\\nYW1lZGEiCiAgICB9LAogICAgImRhdGEiOiAiaHR0cHM6Ly9kYXRhLmFjZ292\\nLm9yZy9hcGkvZ2Vvc3BhdGlhbC84ZTRzLTdmNHY/bWV0aG9kPWV4cG9ydCZm\\nb3JtYXQ9T3JpZ2luYWwiLAogICAgImxpY2Vuc2UiOiAiaHR0cDovL3d3dy5h\\nY2dvdi5vcmcvYWNkYXRhL3Rlcm1zLmh0bSIsCiAgICAiYXR0cmlidXRpb24i\\nOiAiQWxhbWVkYSBDb3VudHkiLAogICAgInllYXIiOiAiIiwKICAgICJ0eXBl\\nIjogImh0dHAiLAogICAgImNvbXByZXNzaW9uIjogInppcCIsCiAgICAiY29u\\nZm9ybSI6IHsKICAgICAgICAibWVyZ2UiOiBbCiAgICAgICAgICAgICJmZWFu\\nbWUiLAogICAgICAgICAgICAiZmVhdHlwIgogICAgICAgIF0sCiAgICAgICAg\\nImxvbiI6ICJ4IiwKICAgICAgICAibGF0IjogInkiLAogICAgICAgICJudW1i\\nZXIiOiAic3RfbnVtIiwKICAgICAgICAic3RyZWV0IjogImF1dG9fc3RyZWV0\\nIiwKICAgICAgICAidHlwZSI6ICJzaGFwZWZpbGUiLAogICAgICAgICJwb3N0\\nY29kZSI6ICJ6aXBjb2RlIgogICAgfQp9Cg==\\n",
              "encoding": "base64",
              "_links": {
                "self": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-alameda_county.json?ref=e91fbc420f08890960f50f863626e1062f922522",
                "git": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs/c9cd0ed30256ae64d5924b03b0423346501b92d8",
                "html": "https://github.com/openaddresses/hooked-on-sources/blob/e91fbc420f08890960f50f863626e1062f922522/sources/us-ca-alameda_county.json"
              }
            }'''
            
            return response(200, data, headers={'Content-Type': 'application/json; charset=utf-8'})
        
        if (url.hostname, url.path) == ('api.github.com', '/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-san_francisco.json') and query.get('ref', '').startswith('ded44e'):
            data = '''{
              "name": "us-ca-san_francisco.json",
              "path": "sources/us-ca-san_francisco.json",
              "sha": "cbf1f900ac072b6a2e728819a97e74bc772e79ff",
              "size": 519,
              "url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-san_francisco.json?ref=ded44ed5f1733bb93d84f94afe9383e2d47bbbaa",
              "html_url": "https://github.com/openaddresses/hooked-on-sources/blob/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa/sources/us-ca-san_francisco.json",
              "git_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs/cbf1f900ac072b6a2e728819a97e74bc772e79ff",
              "download_url": "https://raw.githubusercontent.com/openaddresses/hooked-on-sources/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa/sources/us-ca-san_francisco.json",
              "type": "file",
              "content": "ewogICAgImNvdmVyYWdlIjogewogICAgICAgICJjb3VudHJ5IjogInVzIiwK\\nICAgICAgICAic3RhdGUiOiAiY2EiLAogICAgICAgICJjaXR5IjogIlNhbiBG\\ncmFuY2lzY28iCiAgICB9LAogICAgImF0dHJpYnV0aW9uIjogIkNpdHkgb2Yg\\nU2FuIEZyYW5jaXNjbyIsCiAgICAiZGF0YSI6ICJodHRwczovL2RhdGEuc2Zn\\nb3Yub3JnL2Rvd25sb2FkL2t2ZWotdzVrYi9aSVBQRUQlMjBTSEFQRUZJTEUi\\nLAogICAgImxpY2Vuc2UiOiAiIiwKICAgICJ5ZWFyIjogIiIsCiAgICAidHlw\\nZSI6ICJodHRwIiwKICAgICJjb21wcmVzc2lvbiI6ICJ6aXAiLAogICAgImNv\\nbmZvcm0iOiB7Cgkic3BsaXQiOiAiQUREUkVTUyIsCiAgICAgICAgImxvbiI6\\nICJ4IiwKICAgICAgICAibGF0IjogInkiLAogICAgICAgICJudW1iZXIiOiAi\\nYXV0b19udW1iZXIiLAogICAgICAgICJzdHJlZXQiOiAiYXV0b19zdHJlZXQi\\nLAogICAgICAgICJ0eXBlIjogInNoYXBlZmlsZSIsCiAgICAgICAgInBvc3Rj\\nb2RlIjogInppcGNvZGUiCiAgICB9Cn0K\\n",
              "encoding": "base64",
              "_links": {
                "self": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-san_francisco.json?ref=ded44ed5f1733bb93d84f94afe9383e2d47bbbaa",
                "git": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs/cbf1f900ac072b6a2e728819a97e74bc772e79ff",
                "html": "https://github.com/openaddresses/hooked-on-sources/blob/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa/sources/us-ca-san_francisco.json"
              }
            }'''
            
            return response(200, data, headers={'Content-Type': 'application/json; charset=utf-8'})
        
        if (url.hostname, url.path) == ('api.github.com', '/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-berkeley.json') and query.get('ref', '').startswith('ded44e'):
            data = '''{
              "name": "us-ca-berkeley.json",
              "path": "sources/us-ca-berkeley.json",
              "sha": "16464c39b59b5a09c6526da3afa9a5f57caabcad",
              "size": 779,
              "url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-berkeley.json?ref=ded44ed5f1733bb93d84f94afe9383e2d47bbbaa",
              "html_url": "https://github.com/openaddresses/hooked-on-sources/blob/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa/sources/us-ca-berkeley.json",
              "git_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs/16464c39b59b5a09c6526da3afa9a5f57caabcad",
              "download_url": "https://raw.githubusercontent.com/openaddresses/hooked-on-sources/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa/sources/us-ca-berkeley.json",
              "type": "file",
              "content": "ewogICAgImNvdmVyYWdlIjogewogICAgICAgICJVUyBDZW5zdXMiOiB7CiAg\\nICAgICAgICAgICJnZW9pZCI6ICIwNjA2MDAwIiwKICAgICAgICAgICAgInBs\\nYWNlIjogIkJlcmtlbGV5IiwKICAgICAgICAgICAgInN0YXRlIjogIkNhbGlm\\nb3JuaWEiCiAgICAgICAgfSwKICAgICAgICAiY291bnRyeSI6ICJ1cyIsCiAg\\nICAgICAgInN0YXRlIjogImNhIiwKICAgICAgICAicGxhY2UiOiAiQmVya2Vs\\nZXkiCiAgICB9LAogICAgImF0dHJpYnV0aW9uIjogIkNpdHkgb2YgQmVya2Vs\\nZXkiLAogICAgImRhdGEiOiAiaHR0cDovL3d3dy5jaS5iZXJrZWxleS5jYS51\\ncy91cGxvYWRlZEZpbGVzL0lUL0dJUy9QYXJjZWxzLnppcCIsCiAgICAid2Vi\\nc2l0ZSI6ICJodHRwOi8vd3d3LmNpLmJlcmtlbGV5LmNhLnVzL2RhdGFjYXRh\\nbG9nLyIsCiAgICAidHlwZSI6ICJodHRwIiwKICAgICJjb21wcmVzc2lvbiI6\\nICJ6aXAiLAogICAgIm5vdGUiOiAiTWV0YWRhdGEgYXQgaHR0cDovL3d3dy5j\\naS5iZXJrZWxleS5jYS51cy91cGxvYWRlZEZpbGVzL0lUL0dJUy9QYXJjZWxz\\nLnNocCgxKS54bWwiLAogICAgImNvbmZvcm0iOiB7CiAgICAgICAgImxvbiI6\\nICJ4IiwKICAgICAgICAibGF0IjogInkiLAogICAgICAgICJudW1iZXIiOiAi\\nU3RyZWV0TnVtIiwKICAgICAgICAibWVyZ2UiOiBbIlN0cmVldE5hbWUiLCAi\\nU3RyZWV0U3VmeCIsICJEaXJlY3Rpb24iXSwKICAgICAgICAic3RyZWV0Ijog\\nImF1dG9fc3RyZWV0IiwKICAgICAgICAidHlwZSI6ICJzaGFwZWZpbGUtcG9s\\neWdvbiIKICAgIH0KfQo=\\n",
              "encoding": "base64",
              "_links": {
                "self": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/sources/us-ca-berkeley.json?ref=ded44ed5f1733bb93d84f94afe9383e2d47bbbaa",
                "git": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs/16464c39b59b5a09c6526da3afa9a5f57caabcad",
                "html": "https://github.com/openaddresses/hooked-on-sources/blob/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa/sources/us-ca-berkeley.json"
              }
            }'''
            
            return response(200, data, headers={'Content-Type': 'application/json; charset=utf-8'})
        
        raise ValueError('Unknowable URL "{}"'.format(url.geturl()))

    def test_webhook_one_commit(self):
        '''
        '''
        data = '''{
          "after": "e91fbc420f08890960f50f863626e1062f922522", 
          "base_ref": null, 
          "before": "c52204fd40f17f9da243df09e6d1107d48768afd", 
          "commits": [
            {
              "added": [
                "sources/us-ca-alameda_county.json"
              ], 
              "author": {
                "email": "mike@teczno.com", 
                "name": "Michal Migurski", 
                "username": "migurski"
              }, 
              "committer": {
                "email": "mike@teczno.com", 
                "name": "Michal Migurski", 
                "username": "migurski"
              }, 
              "distinct": true, 
              "id": "e91fbc420f08890960f50f863626e1062f922522", 
              "message": "Added first source", 
              "modified": [], 
              "removed": [], 
              "timestamp": "2015-04-25T17:16:12-07:00", 
              "url": "https://github.com/openaddresses/hooked-on-sources/commit/e91fbc420f08890960f50f863626e1062f922522"
            }
          ], 
          "compare": "https://github.com/openaddresses/hooked-on-sources/compare/c52204fd40f1...e91fbc420f08", 
          "created": false, 
          "deleted": false, 
          "forced": false, 
          "head_commit": {
            "added": [
              "sources/us-ca-alameda_county.json"
            ], 
            "author": {
              "email": "mike@teczno.com", 
              "name": "Michal Migurski", 
              "username": "migurski"
            }, 
            "committer": {
              "email": "mike@teczno.com", 
              "name": "Michal Migurski", 
              "username": "migurski"
            }, 
            "distinct": true, 
            "id": "e91fbc420f08890960f50f863626e1062f922522", 
            "message": "Added first source", 
            "modified": [], 
            "removed": [], 
            "timestamp": "2015-04-25T17:16:12-07:00", 
            "url": "https://github.com/openaddresses/hooked-on-sources/commit/e91fbc420f08890960f50f863626e1062f922522"
          }, 
          "organization": {
            "avatar_url": "https://avatars.githubusercontent.com/u/6895392?v=3", 
            "description": "The free and open global address collection ", 
            "events_url": "https://api.github.com/orgs/openaddresses/events", 
            "id": 6895392, 
            "login": "openaddresses", 
            "members_url": "https://api.github.com/orgs/openaddresses/members{/member}", 
            "public_members_url": "https://api.github.com/orgs/openaddresses/public_members{/member}", 
            "repos_url": "https://api.github.com/orgs/openaddresses/repos", 
            "url": "https://api.github.com/orgs/openaddresses"
          }, 
          "pusher": {
            "email": "mike-github@teczno.com", 
            "name": "migurski"
          }, 
          "ref": "refs/heads/master", 
          "repository": {
            "archive_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/{archive_format}{/ref}", 
            "assignees_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/assignees{/user}", 
            "blobs_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs{/sha}", 
            "branches_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/branches{/branch}", 
            "clone_url": "https://github.com/openaddresses/hooked-on-sources.git", 
            "collaborators_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/collaborators{/collaborator}", 
            "comments_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/comments{/number}", 
            "commits_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/commits{/sha}", 
            "compare_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/compare/{base}...{head}", 
            "contents_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/{+path}", 
            "contributors_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contributors", 
            "created_at": 1430006167, 
            "default_branch": "master", 
            "description": "Temporary repository for testing Github webhook features", 
            "downloads_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/downloads", 
            "events_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/events", 
            "fork": false, 
            "forks": 0, 
            "forks_count": 0, 
            "forks_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/forks", 
            "full_name": "openaddresses/hooked-on-sources", 
            "git_commits_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/commits{/sha}", 
            "git_refs_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/refs{/sha}", 
            "git_tags_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/tags{/sha}", 
            "git_url": "git://github.com/openaddresses/hooked-on-sources.git", 
            "has_downloads": true, 
            "has_issues": true, 
            "has_pages": false, 
            "has_wiki": true, 
            "homepage": null, 
            "hooks_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/hooks", 
            "html_url": "https://github.com/openaddresses/hooked-on-sources", 
            "id": 34590951, 
            "issue_comment_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues/comments{/number}", 
            "issue_events_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues/events{/number}", 
            "issues_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues{/number}", 
            "keys_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/keys{/key_id}", 
            "labels_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/labels{/name}", 
            "language": null, 
            "languages_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/languages", 
            "master_branch": "master", 
            "merges_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/merges", 
            "milestones_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/milestones{/number}", 
            "mirror_url": null, 
            "name": "hooked-on-sources", 
            "notifications_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/notifications{?since,all,participating}", 
            "open_issues": 0, 
            "open_issues_count": 0, 
            "organization": "openaddresses", 
            "owner": {
              "email": "openaddresses@gmail.com", 
              "name": "openaddresses"
            }, 
            "private": false, 
            "pulls_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/pulls{/number}", 
            "pushed_at": 1430007676, 
            "releases_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/releases{/id}", 
            "size": 0, 
            "ssh_url": "git@github.com:openaddresses/hooked-on-sources.git", 
            "stargazers": 0, 
            "stargazers_count": 0, 
            "stargazers_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/stargazers", 
            "statuses_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/statuses/{sha}", 
            "subscribers_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/subscribers", 
            "subscription_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/subscription", 
            "svn_url": "https://github.com/openaddresses/hooked-on-sources", 
            "tags_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/tags", 
            "teams_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/teams", 
            "trees_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/trees{/sha}", 
            "updated_at": "2015-04-25T23:56:07Z", 
            "url": "https://github.com/openaddresses/hooked-on-sources", 
            "watchers": 0, 
            "watchers_count": 0
          }, 
          "sender": {
            "avatar_url": "https://avatars.githubusercontent.com/u/58730?v=3", 
            "events_url": "https://api.github.com/users/migurski/events{/privacy}", 
            "followers_url": "https://api.github.com/users/migurski/followers", 
            "following_url": "https://api.github.com/users/migurski/following{/other_user}", 
            "gists_url": "https://api.github.com/users/migurski/gists{/gist_id}", 
            "gravatar_id": "", 
            "html_url": "https://github.com/migurski", 
            "id": 58730, 
            "login": "migurski", 
            "organizations_url": "https://api.github.com/users/migurski/orgs", 
            "received_events_url": "https://api.github.com/users/migurski/received_events", 
            "repos_url": "https://api.github.com/users/migurski/repos", 
            "site_admin": false, 
            "starred_url": "https://api.github.com/users/migurski/starred{/owner}{/repo}", 
            "subscriptions_url": "https://api.github.com/users/migurski/subscriptions", 
            "type": "User", 
            "url": "https://api.github.com/users/migurski"
          }
        }'''
        
        with HTTMock(self.response_content):
            posted = self.client.post('/hook', data=data)
        
        self.assertEqual(posted.status_code, 200)
        self.assertTrue('us-ca-alameda_county' in posted.data)
        self.assertTrue('data.acgov.org' in posted.data)

    def test_webhook_two_commits(self):
        '''
        '''
        data = '''{
          "after": "ded44ed5f1733bb93d84f94afe9383e2d47bbbaa", 
          "base_ref": null, 
          "before": "e91fbc420f08890960f50f863626e1062f922522", 
          "commits": [
            {
              "added": [
                "sources/us-ca-san_francisco.json"
              ], 
              "author": {
                "email": "mike@teczno.com", 
                "name": "Michal Migurski", 
                "username": "migurski"
              }, 
              "committer": {
                "email": "mike@teczno.com", 
                "name": "Michal Migurski", 
                "username": "migurski"
              }, 
              "distinct": true, 
              "id": "73a81c5b337bd393273a222f1cd191d7e634df51", 
              "message": "Added SF", 
              "modified": [], 
              "removed": [], 
              "timestamp": "2015-04-25T17:25:45-07:00", 
              "url": "https://github.com/openaddresses/hooked-on-sources/commit/73a81c5b337bd393273a222f1cd191d7e634df51"
            }, 
            {
              "added": [
                "sources/us-ca-berkeley.json"
              ], 
              "author": {
                "email": "mike@teczno.com", 
                "name": "Michal Migurski", 
                "username": "migurski"
              }, 
              "committer": {
                "email": "mike@teczno.com", 
                "name": "Michal Migurski", 
                "username": "migurski"
              }, 
              "distinct": true, 
              "id": "ded44ed5f1733bb93d84f94afe9383e2d47bbbaa", 
              "message": "Added Berkeley", 
              "modified": [], 
              "removed": [], 
              "timestamp": "2015-04-25T17:25:55-07:00", 
              "url": "https://github.com/openaddresses/hooked-on-sources/commit/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa"
            }
          ], 
          "compare": "https://github.com/openaddresses/hooked-on-sources/compare/e91fbc420f08...ded44ed5f173", 
          "created": false, 
          "deleted": false, 
          "forced": false, 
          "head_commit": {
            "added": [
              "sources/us-ca-berkeley.json"
            ], 
            "author": {
              "email": "mike@teczno.com", 
              "name": "Michal Migurski", 
              "username": "migurski"
            }, 
            "committer": {
              "email": "mike@teczno.com", 
              "name": "Michal Migurski", 
              "username": "migurski"
            }, 
            "distinct": true, 
            "id": "ded44ed5f1733bb93d84f94afe9383e2d47bbbaa", 
            "message": "Added Berkeley", 
            "modified": [], 
            "removed": [], 
            "timestamp": "2015-04-25T17:25:55-07:00", 
            "url": "https://github.com/openaddresses/hooked-on-sources/commit/ded44ed5f1733bb93d84f94afe9383e2d47bbbaa"
          }, 
          "organization": {
            "avatar_url": "https://avatars.githubusercontent.com/u/6895392?v=3", 
            "description": "The free and open global address collection ", 
            "events_url": "https://api.github.com/orgs/openaddresses/events", 
            "id": 6895392, 
            "login": "openaddresses", 
            "members_url": "https://api.github.com/orgs/openaddresses/members{/member}", 
            "public_members_url": "https://api.github.com/orgs/openaddresses/public_members{/member}", 
            "repos_url": "https://api.github.com/orgs/openaddresses/repos", 
            "url": "https://api.github.com/orgs/openaddresses"
          }, 
          "pusher": {
            "email": "mike-github@teczno.com", 
            "name": "migurski"
          }, 
          "ref": "refs/heads/master", 
          "repository": {
            "archive_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/{archive_format}{/ref}", 
            "assignees_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/assignees{/user}", 
            "blobs_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs{/sha}", 
            "branches_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/branches{/branch}", 
            "clone_url": "https://github.com/openaddresses/hooked-on-sources.git", 
            "collaborators_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/collaborators{/collaborator}", 
            "comments_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/comments{/number}", 
            "commits_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/commits{/sha}", 
            "compare_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/compare/{base}...{head}", 
            "contents_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/{+path}", 
            "contributors_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contributors", 
            "created_at": 1430006167, 
            "default_branch": "master", 
            "description": "Temporary repository for testing Github webhook features", 
            "downloads_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/downloads", 
            "events_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/events", 
            "fork": false, 
            "forks": 0, 
            "forks_count": 0, 
            "forks_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/forks", 
            "full_name": "openaddresses/hooked-on-sources", 
            "git_commits_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/commits{/sha}", 
            "git_refs_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/refs{/sha}", 
            "git_tags_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/tags{/sha}", 
            "git_url": "git://github.com/openaddresses/hooked-on-sources.git", 
            "has_downloads": true, 
            "has_issues": true, 
            "has_pages": false, 
            "has_wiki": true, 
            "homepage": null, 
            "hooks_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/hooks", 
            "html_url": "https://github.com/openaddresses/hooked-on-sources", 
            "id": 34590951, 
            "issue_comment_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues/comments{/number}", 
            "issue_events_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues/events{/number}", 
            "issues_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues{/number}", 
            "keys_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/keys{/key_id}", 
            "labels_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/labels{/name}", 
            "language": null, 
            "languages_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/languages", 
            "master_branch": "master", 
            "merges_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/merges", 
            "milestones_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/milestones{/number}", 
            "mirror_url": null, 
            "name": "hooked-on-sources", 
            "notifications_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/notifications{?since,all,participating}", 
            "open_issues": 0, 
            "open_issues_count": 0, 
            "organization": "openaddresses", 
            "owner": {
              "email": "openaddresses@gmail.com", 
              "name": "openaddresses"
            }, 
            "private": false, 
            "pulls_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/pulls{/number}", 
            "pushed_at": 1430007964, 
            "releases_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/releases{/id}", 
            "size": 0, 
            "ssh_url": "git@github.com:openaddresses/hooked-on-sources.git", 
            "stargazers": 0, 
            "stargazers_count": 0, 
            "stargazers_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/stargazers", 
            "statuses_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/statuses/{sha}", 
            "subscribers_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/subscribers", 
            "subscription_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/subscription", 
            "svn_url": "https://github.com/openaddresses/hooked-on-sources", 
            "tags_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/tags", 
            "teams_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/teams", 
            "trees_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/trees{/sha}", 
            "updated_at": "2015-04-25T23:56:07Z", 
            "url": "https://github.com/openaddresses/hooked-on-sources", 
            "watchers": 0, 
            "watchers_count": 0
          }, 
          "sender": {
            "avatar_url": "https://avatars.githubusercontent.com/u/58730?v=3", 
            "events_url": "https://api.github.com/users/migurski/events{/privacy}", 
            "followers_url": "https://api.github.com/users/migurski/followers", 
            "following_url": "https://api.github.com/users/migurski/following{/other_user}", 
            "gists_url": "https://api.github.com/users/migurski/gists{/gist_id}", 
            "gravatar_id": "", 
            "html_url": "https://github.com/migurski", 
            "id": 58730, 
            "login": "migurski", 
            "organizations_url": "https://api.github.com/users/migurski/orgs", 
            "received_events_url": "https://api.github.com/users/migurski/received_events", 
            "repos_url": "https://api.github.com/users/migurski/repos", 
            "site_admin": false, 
            "starred_url": "https://api.github.com/users/migurski/starred{/owner}{/repo}", 
            "subscriptions_url": "https://api.github.com/users/migurski/subscriptions", 
            "type": "User", 
            "url": "https://api.github.com/users/migurski"
          }
        }'''
        
        with HTTMock(self.response_content):
            posted = self.client.post('/hook', data=data)
        
        self.assertEqual(posted.status_code, 200)
        self.assertTrue('us-ca-san_francisco' in posted.data)
        self.assertTrue('data.sfgov.org' in posted.data)
        self.assertTrue('us-ca-berkeley' in posted.data)
        self.assertTrue('www.ci.berkeley.ca.us' in posted.data)

    def test_webhook_add_remove(self):
        '''
        '''
        data = '''{
          "ref": "refs/heads/branch",
          "before": "b659130053b85cd3993b1a4653da1bf6231ec0b4",
          "after": "e5f1dcae83ab1ef1f736b969da617311f7f11564",
          "created": false,
          "deleted": false,
          "forced": false,
          "base_ref": null,
          "compare": "https://github.com/openaddresses/hooked-on-sources/compare/b659130053b8...e5f1dcae83ab",
          "commits": [
            {
              "id": "0cbd51b8f6044e98c919dcabf93e3f4e1d58c035",
              "distinct": true,
              "message": "Added Polish source",
              "timestamp": "2015-04-25T17:52:39-07:00",
              "url": "https://github.com/openaddresses/hooked-on-sources/commit/0cbd51b8f6044e98c919dcabf93e3f4e1d58c035",
              "author": {
                "name": "Michal Migurski",
                "email": "mike@teczno.com",
                "username": "migurski"
              },
              "committer": {
                "name": "Michal Migurski",
                "email": "mike@teczno.com",
                "username": "migurski"
              },
              "added": [
                "sources/pl-dolnoslaskie.json"
              ],
              "removed": [

              ],
              "modified": [

              ]
            },
            {
              "id": "e5f1dcae83ab1ef1f736b969da617311f7f11564",
              "distinct": true,
              "message": "Removed Polish source",
              "timestamp": "2015-04-25T17:52:46-07:00",
              "url": "https://github.com/openaddresses/hooked-on-sources/commit/e5f1dcae83ab1ef1f736b969da617311f7f11564",
              "author": {
                "name": "Michal Migurski",
                "email": "mike@teczno.com",
                "username": "migurski"
              },
              "committer": {
                "name": "Michal Migurski",
                "email": "mike@teczno.com",
                "username": "migurski"
              },
              "added": [

              ],
              "removed": [
                "sources/pl-dolnoslaskie.json"
              ],
              "modified": [

              ]
            }
          ],
          "head_commit": {
            "id": "e5f1dcae83ab1ef1f736b969da617311f7f11564",
            "distinct": true,
            "message": "Removed Polish source",
            "timestamp": "2015-04-25T17:52:46-07:00",
            "url": "https://github.com/openaddresses/hooked-on-sources/commit/e5f1dcae83ab1ef1f736b969da617311f7f11564",
            "author": {
              "name": "Michal Migurski",
              "email": "mike@teczno.com",
              "username": "migurski"
            },
            "committer": {
              "name": "Michal Migurski",
              "email": "mike@teczno.com",
              "username": "migurski"
            },
            "added": [

            ],
            "removed": [
              "sources/pl-dolnoslaskie.json"
            ],
            "modified": [

            ]
          },
          "repository": {
            "id": 34590951,
            "name": "hooked-on-sources",
            "full_name": "openaddresses/hooked-on-sources",
            "owner": {
              "name": "openaddresses",
              "email": "openaddresses@gmail.com"
            },
            "private": false,
            "html_url": "https://github.com/openaddresses/hooked-on-sources",
            "description": "Temporary repository for testing Github webhook features",
            "fork": false,
            "url": "https://github.com/openaddresses/hooked-on-sources",
            "forks_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/forks",
            "keys_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/keys{/key_id}",
            "collaborators_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/collaborators{/collaborator}",
            "teams_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/teams",
            "hooks_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/hooks",
            "issue_events_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues/events{/number}",
            "events_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/events",
            "assignees_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/assignees{/user}",
            "branches_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/branches{/branch}",
            "tags_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/tags",
            "blobs_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/blobs{/sha}",
            "git_tags_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/tags{/sha}",
            "git_refs_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/refs{/sha}",
            "trees_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/trees{/sha}",
            "statuses_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/statuses/{sha}",
            "languages_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/languages",
            "stargazers_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/stargazers",
            "contributors_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contributors",
            "subscribers_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/subscribers",
            "subscription_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/subscription",
            "commits_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/commits{/sha}",
            "git_commits_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/git/commits{/sha}",
            "comments_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/comments{/number}",
            "issue_comment_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues/comments{/number}",
            "contents_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/contents/{+path}",
            "compare_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/compare/{base}...{head}",
            "merges_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/merges",
            "archive_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/{archive_format}{/ref}",
            "downloads_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/downloads",
            "issues_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/issues{/number}",
            "pulls_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/pulls{/number}",
            "milestones_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/milestones{/number}",
            "notifications_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/notifications{?since,all,participating}",
            "labels_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/labels{/name}",
            "releases_url": "https://api.github.com/repos/openaddresses/hooked-on-sources/releases{/id}",
            "created_at": 1430006167,
            "updated_at": "2015-04-25T23:56:07Z",
            "pushed_at": 1430009572,
            "git_url": "git://github.com/openaddresses/hooked-on-sources.git",
            "ssh_url": "git@github.com:openaddresses/hooked-on-sources.git",
            "clone_url": "https://github.com/openaddresses/hooked-on-sources.git",
            "svn_url": "https://github.com/openaddresses/hooked-on-sources",
            "homepage": null,
            "size": 0,
            "stargazers_count": 0,
            "watchers_count": 0,
            "language": null,
            "has_issues": true,
            "has_downloads": true,
            "has_wiki": true,
            "has_pages": false,
            "forks_count": 0,
            "mirror_url": null,
            "open_issues_count": 1,
            "forks": 0,
            "open_issues": 1,
            "watchers": 0,
            "default_branch": "master",
            "stargazers": 0,
            "master_branch": "master",
            "organization": "openaddresses"
          },
          "pusher": {
            "name": "migurski",
            "email": "mike-github@teczno.com"
          },
          "organization": {
            "login": "openaddresses",
            "id": 6895392,
            "url": "https://api.github.com/orgs/openaddresses",
            "repos_url": "https://api.github.com/orgs/openaddresses/repos",
            "events_url": "https://api.github.com/orgs/openaddresses/events",
            "members_url": "https://api.github.com/orgs/openaddresses/members{/member}",
            "public_members_url": "https://api.github.com/orgs/openaddresses/public_members{/member}",
            "avatar_url": "https://avatars.githubusercontent.com/u/6895392?v=3",
            "description": "The free and open global address collection "
          },
          "sender": {
            "login": "migurski",
            "id": 58730,
            "avatar_url": "https://avatars.githubusercontent.com/u/58730?v=3",
            "gravatar_id": "",
            "url": "https://api.github.com/users/migurski",
            "html_url": "https://github.com/migurski",
            "followers_url": "https://api.github.com/users/migurski/followers",
            "following_url": "https://api.github.com/users/migurski/following{/other_user}",
            "gists_url": "https://api.github.com/users/migurski/gists{/gist_id}",
            "starred_url": "https://api.github.com/users/migurski/starred{/owner}{/repo}",
            "subscriptions_url": "https://api.github.com/users/migurski/subscriptions",
            "organizations_url": "https://api.github.com/users/migurski/orgs",
            "repos_url": "https://api.github.com/users/migurski/repos",
            "events_url": "https://api.github.com/users/migurski/events{/privacy}",
            "received_events_url": "https://api.github.com/users/migurski/received_events",
            "type": "User",
            "site_admin": false
          }
        }'''
        
        with HTTMock(self.response_content):
            posted = self.client.post('/hook', data=data)
        
        self.assertEqual(posted.status_code, 200)
        self.assertFalse('pl-dolnoslaskie' in posted.data)

if __name__ == '__main__':
    unittest.main()
