"""Invalid requestor / Jira ticket body overrides shared by the base-prices
and inflation-index request-validation tests."""

INVALID_REQUESTOR_JIRA_TICKET_OVERRIDES = [
    {"requestor": "   "},
    {"requestor": "x" * 256},
    {"jiraTicket": "nrf2-1234"},
    {"jiraTicket": "ABC-1234"},
    {"jiraTicket": "NRF2-"},
    {"jiraTicket": "NRF2-12a"},
    {"jiraTicket": "NRF2-1234\n"},
    {"jiraTicket": " NRF2-1234"},
    {"jiraTicket": "NRF2-" + "1" * 20},
]
