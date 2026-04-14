
from typing import TypedDict

from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from typing import Annotated, List
from langgraph.graph import MessagesState, StateGraph, add_messages, END 
from langgraph.prebuilt import ToolNode
load_dotenv()
import asyncio

from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI

mcp_client = MultiServerMCPClient(
    {
        # ,
        "databricks": {
            "command": "uv",
            "args": ["run",  "--directory", "/home/ash/code_projects/ai-dev-kit", "python", "databricks-mcp-server/run_server.py"],
            # "defer_loading": True,
            "transport": "stdio"
        }
    }
)

async def get_llm_with_tools():
    llm = ChatOpenAI(model="gpt-4o-mini")
    tools = await mcp_client.get_tools()
    return llm.bind_tools(tools), tools


class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]

SYSTEM_PROMPT = "You are a helpful Databricks assistant."

async def agent_node(state, llm_with_tools):
    messages = [SystemMessage(SYSTEM_PROMPT)] + state["messages"]
    resp = await llm_with_tools.ainvoke(messages)
    return {"messages": [resp] }

def make_tool_node(tools):
    return ToolNode(tools, handle_tool_errors=True)

def router(state: AgentState):
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    return END

async def build_graph():

    llm_with_tools, tools = await get_llm_with_tools()

    async def _agent(state):
        """ This will be the Node, part of the graph"""         
        return await agent_node(state, llm_with_tools)

    graph=StateGraph(AgentState)

    graph.add_node("agent", _agent)
    graph.add_node("tools", make_tool_node(tools))

    graph.add_conditional_edges("agent", router, {"tools":"tools", END:END})
    graph.add_edge("tools","agent")

    graph.set_entry_point("agent")

    return graph.compile()

async def main():
    app = await build_graph()
    while True:
        user_input = input("$user: ")
        if any(keyword in user_input.lower() for keyword in ["exit", "done"]):
            break
        async for chunk in app.astream({"messages" : [HumanMessage(user_input)] },
                                    stream_mode="updates"):
            for node, state in chunk.items():
                print("\n $", node)
                for msg in state["messages"]:
                    print(msg.content or msg.tool_calls)

if __name__ == '__main__':
    asyncio.run(main())

