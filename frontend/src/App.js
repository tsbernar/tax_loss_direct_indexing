import React from 'react';
import { template_dark } from './PlotlyTemplate';
import {
  ChakraProvider,
  InputGroup,
  extendTheme,
  Input,
  InputRightElement,
  Button,
  useColorModeValue,
  Box,
  Text,
  Table,
  Tbody,
  Thead,
  Tr,
  Th,
  Td,
  Spinner,
  Grid,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  TableContainer,
} from '@chakra-ui/react';
import { ColorModeSwitcher } from './ColorModeSwitcher';
import Plot from 'react-plotly.js';

async function logoutAPI() {
  console.log('logging out');
  return fetch('api/auth/logout', {
    credentials: 'include',
    method: 'GET',
  }).then(response => {
    return response.ok;
  });
}

async function authAPIData(pwd) {
  return fetch('api/auth', {
    credentials: 'include',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ pw: pwd }),
  }).then(response => {
    return response.ok;
  });
}

async function getAPIData(endpoint) {
  return fetch('api/' + endpoint, {
    credentials: 'include',
    method: 'GET',
  })
    .then(response => {
      if (!response.ok) {
        if (response.status === 403) {
          return response.json();
        }
        throw new Error(
          `Could not fetch data from api. Status: ${response.status}: ${response.statusText}`
        );
      }
      return response.json();
    })
    .then(jsonData => {
      return jsonData;
    });
}

function ReturnPlot(props) {
  const isDarkMode = useColorModeValue(false, true);
  var pf_returns = {
    y: props.return_data['pf_returns'].map(item => item.return),
    x: props.return_data['pf_returns'].map(item => item.date),
    type: 'scatter',
    mode: 'line',
    name: 'Portfolio returns',
    color: 'red',
  };
  var index_returns = {
    y: props.return_data['index_returns'].map(item => item.return),
    x: props.return_data['index_returns'].map(item => item.date),
    type: 'scatter',
    mode: 'line',
    name: 'Index returns',
    color: 'blue',
  };
  return (
    <Plot
      data={[pf_returns, index_returns]}
      layout={{
        title: 'Portfolio vs Index Returns',
        template: isDarkMode ? template_dark : {},
        margin: {
          l: 5,
          r: 5,
        },
        yaxis: {
          tickformat: ',.0%',
          fixedrange: true,
          automargin: true,
        },
        legend: { yanchor: 'top', y: -0.1, x: 1, xanchor: 'right' },
        xaxis: {
          fixedrange: true,
          rangeselector: {
            font: { color: 'black' },
            buttons: [
              {
                count: 1,
                label: '1m',
                step: 'month',
                stepmode: 'backward',
              },
              {
                count: 6,
                label: '6m',
                step: 'month',
                stepmode: 'backward',
              },
              { step: 'all' },
            ],
          },
        },
      }}
      useResizeHandler={true}
      style={{ width: '100%', height: '80%' }}
      config={{ displayModeBar: false }}
    />
  );
}

function PasswordInput(props) {
  const [show, setShow] = React.useState(false);
  const handleClick = () => setShow(!show);

  let text = 'Auth Required';
  if ('text' in props) {
    text += props.text;
  }
  return (
    <Box>
      <Text>{text}</Text>
      <InputGroup size="md" maxW="30rem">
        <Input
          pr="4.5rem"
          type={show ? 'text' : 'password'}
          placeholder="Enter password"
          onKeyPress={props.handleKeyPress}
        />
        <InputRightElement width="4.5rem">
          <Button h="1.75rem" size="sm" onClick={handleClick}>
            {show ? 'Hide' : 'Show'}
          </Button>
        </InputRightElement>
      </InputGroup>
    </Box>
  );
}

function Loading() {
  return (
    <Spinner
      thickness="4px"
      speed="0.65s"
      emptyColor="gray.200"
      color="blue.500"
      size="xl"
    />
  );
}

class Holdings extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      sorted_positions: [],
      ascending: true,
    };
    this.sort_positions = this.sort_positions.bind(this);
  }

  sort_positions(sort_key) {
    const ascending = !this.state.ascending;
    const mult = ascending ? 1 : -1;
    let sorted_positions = [...this.props.holdings_data.positions];
    sorted_positions.sort((a_, b_) => {
      let a = a_[sort_key];
      let b = b_[sort_key];
      // first remove $ from cash fields
      if (typeof a === 'string' && a.length && a[0] === '$') {
        a = a.slice(2).replace(',', '');
      }
      if (typeof b === 'string' && b.length && b[0] === '$') {
        b = b.slice(2).replace(',', '');
      }
      // convert to number if numeric
      if (!isNaN(a) && !isNaN(b)) {
        a = Number(a);
        b = Number(b);
      }
      if (a < b) {
        return -1 * mult;
      }
      if (a > b) {
        return 1 * mult;
      }
      return 0;
    });
    this.setState({
      sorted_positions: sorted_positions,
      ascending: ascending,
    });
  }

  render() {
    if (!this.props.holdings_loaded) {
      return <Loading></Loading>;
    }
    if (this.props.auth_required) {
      return (
        <PasswordInput
          handleKeyPress={this.props.handlePwdKeyPress}
          text={this.props.error ? ' | Error: ' + this.props.error_data : ''}
        />
      );
    } else if (this.props.error) {
      return <Text>{this.props.error_data}</Text>;
    }

    let positions = this.props.holdings_data.positions;
    if (this.state.sorted_positions.length) {
      positions = this.state.sorted_positions;
    }

    return (
      <Box width={['100wv', '100vw', '100vw', '80vw']}>
        <Text>
          {' '}
          NAV: $
          {Intl.NumberFormat('en-US').format(
            this.props.holdings_data.nav.toFixed(2)
          )}{' '}
        </Text>
        <TableContainer>
          <Table variant="striped" fontSize="xs" size="xs" overflowX="scroll">
            <Thead>
              <Tr w="14%">
                <Th>
                  <Button
                    size="xs"
                    onClick={() => this.sort_positions('ticker')}
                  >
                    TICKER ↕️
                  </Button>
                </Th>
                <Th w="14%">
                  <Button
                    size="xs"
                    onClick={() => this.sort_positions('total_shares')}
                  >
                    SHARES ↕️
                  </Button>
                </Th>
                <Th w="14%">
                  <Button
                    size="xs"
                    onClick={() =>
                      this.sort_positions('total_shares_with_loss')
                    }
                  >
                    W/ LOSS ↕️
                  </Button>
                </Th>
                <Th w="14%">
                  <Button
                    size="xs"
                    onClick={() => this.sort_positions('total_gain/loss')}
                  >
                    uPNL ↕️
                  </Button>
                </Th>
                <Th w="14%">
                  <Button
                    size="xs"
                    onClick={() => this.sort_positions('market_price')}
                  >
                    PRICE ↕️
                  </Button>
                </Th>
                <Th w="14%">
                  <Button
                    size="xs"
                    onClick={() => this.sort_positions('market_value')}
                  >
                    VALUE ↕️
                  </Button>
                </Th>
                <Th w="14%">
                  <Button size="xs" onClick={() => this.sort_positions('%')}>
                    % ↕️
                  </Button>
                </Th>
              </Tr>
            </Thead>
            <Tbody>
              {positions.map(position => {
                return (
                  <Tr key={position.ticker}>
                    <Td>{position.ticker}</Td>
                    <Td>{Number(position.total_shares)}</Td>
                    <Td>{Number(position.total_shares_with_loss)}</Td>
                    <Td>{position['total_gain/loss']}</Td>
                    <Td>{position.market_price}</Td>
                    <Td>{position.market_value}</Td>
                    <Td>{Number(position['%'])}</Td>
                  </Tr>
                );
              })}
              <Tr key="Total">
                <Td>Total</Td>
                <Td>
                  {positions.reduce((sum, position) => {
                    return sum + Number(position.total_shares);
                  }, 0)}
                </Td>
                <Td>
                  {positions.reduce((sum, position) => {
                    return sum + Number(position.total_shares_with_loss);
                  }, 0)}
                </Td>
                <Td>
                  {positions
                    .reduce((sum, position) => {
                      return sum + Number(position['total_gain/loss'].slice(1));
                    }, 0)
                    .toFixed(2)}
                </Td>
                <Td>-</Td>
                <Td>
                  {positions
                    .reduce((sum, position) => {
                      return sum + Number(position.market_value.slice(1));
                    }, 0)
                    .toFixed(2)}
                </Td>
                <Td>-</Td>
              </Tr>
            </Tbody>
          </Table>
        </TableContainer>
      </Box>
    );
  }
}
class Returns extends React.Component {
  render() {
    if (!this.props.returns_loaded) {
      return <Loading></Loading>;
    }
    if (this.props.auth_required) {
      return (
        <PasswordInput
          handleKeyPress={this.props.handlePwdKeyPress}
          text={this.props.error ? ' | Error: ' + this.props.error_data : ''}
        />
      );
    } else if (this.props.error) {
      return <Text>{this.props.error_data}</Text>;
    }
    return (
      <Box width={['100wv', '100vw', '100vw', '80vw']}>
        <ReturnPlot return_data={this.props.return_data}> </ReturnPlot>
      </Box>
    );
  }
}

function Menu(props) {
  let button = null;
  if (!props.auth_required) {
    button = <Button onClick={props.logout}>Logout</Button>;
  }
  return (
    <Box>
      {button}
      <Tabs align="center">
        <TabList>
          <Tab>Returns</Tab>
          <Tab>Holdings</Tab>
          <Tab>Parameters</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <Returns {...props} />
          </TabPanel>
          <TabPanel>
            <Holdings {...props} />
          </TabPanel>
          <TabPanel>
            <Text> Parameters </Text>
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );
}

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      return_data: 'Loading',
      returns_loaded: false,
      holdings_loaded: false,
      params_loaded: false,
      error: false,
      error_data: 'Unknown error',
      auth_required: false,
    };

    // Don't really understand this yet, but needed to access "this" in functions
    this.logout = this.logout.bind(this);
    this.handlePwdKeyPress = this.handlePwdKeyPress.bind(this);
    this.requestData = this.requestData.bind(this);
  }

  handlePwdKeyPress(e) {
    if (e.key === 'Enter') {
      const pwd = e.target.value;
      authAPIData(pwd).then(success => {
        if (success) {
          this.setState({
            auth_required: false,
            error: false,
            error_data: '',
            returns_loaded: false,
            holdings_loaded: false,
            params_loaded: false,
          });
          this.requestData();
        } else {
          this.setState({ error: true, error_data: 'Incorrect password' });
        }
      });
    }
  }

  logout() {
    logoutAPI();
    this.setState({ error: false, auth_required: true });
  }

  requestData() {
    console.log('requesting return data');
    getAPIData('returns')
      .then(jsonData => {
        if ('index_returns' in jsonData) {
          this.setState({ return_data: jsonData, returns_loaded: true });
        } else if (
          'message' in jsonData &&
          jsonData['message'].startsWith('Not authenticated')
        ) {
          this.setState({
            returns_loaded: true,
            holdings_loaded: true,
            params_loaded: true,
            auth_required: true,
          });
          return;
        }
      })
      .catch(err => {
        console.log(`error ${err}`);
        if (err) {
          this.setState({ error_data: String(err) });
        }
        this.setState({ error: true, returns_loaded: true });
      });
    console.log('requeting holdings data');
    getAPIData('holdings')
      .then(jsonData => {
        if ('positions' in jsonData) {
          this.setState({ holdings_data: jsonData, holdings_loaded: true });
        } else if (
          'message' in jsonData &&
          jsonData['message'].startsWith('Not authenticated')
        ) {
          this.setState({
            returns_loaded: true,
            holdings_loaded: true,
            params_loaded: true,
            auth_required: true,
          });
          return;
        }
      })
      .catch(err => {
        console.log(`error ${err}`);
        if (err) {
          this.setState({ error_data: String(err) });
        }
        this.setState({ error: true, holdings_loaded: true });
      });
  }

  componentDidMount() {
    console.log(`mount ${JSON.stringify(this.state)})`);
    if (!(this.state.returns_loaded && this.state.holdings_loaded)) {
      this.requestData();
    }
  }

  render() {
    const config = {
      useSystemColorMode: false,
      initialColorMode: 'dark',
    };
    const customTheme = extendTheme({ config });

    return (
      <ChakraProvider theme={customTheme}>
        <Box textAlign="right" fontSize="xl">
          <Grid minH="10vh" p={3}>
            <ColorModeSwitcher justifySelf="flex-end" />
            <Menu
              {...this.state}
              logout={this.logout}
              handlePwdKeyPress={this.handlePwdKeyPress}
            />
          </Grid>
        </Box>
      </ChakraProvider>
    );
  }
}

export default App;
